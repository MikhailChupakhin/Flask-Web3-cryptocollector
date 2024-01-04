# ModuleTokenTXProcessor/__init__.py
import asyncio
import logging
import os
import time
from logging.handlers import RotatingFileHandler

import redis
import sys
from celery import Celery, chain
from celery.exceptions import Ignore
from celery.schedules import crontab
from flask import Flask
from flask_socketio import SocketIO

from ModuleBlockAnalyzer import send_transaction_to_rabbitmq
from ModuleBlockAnalyzer.models import UserWallet
import json
import pickle
import aio_pika
from pathlib import Path

from ModuleTokenTXProcessor.singltones import Web3Singleton, DatabaseSingleton

PROVIDER_URL = os.environ.get('PROVIDER_URL')

COLD_WALLET = os.environ.get('COLD_WALLET')
GAS_KEEPER = os.environ.get('GAS_KEEPER')
GAS_KEEPER_PK = os.environ.get('GAS_KEEPER_PK')
GAS_KEEPER_ALERT_VALUE = int(os.environ.get('GAS_KEEPER_ALERT_VALUE'))
CHECK_GAS_LACK_TASK_FREQUENCY = int(os.environ.get('CHECK_GAS_LACK_TASK_FREQUENCY'))

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL')
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND')
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')
REDIS_DB = os.environ.get('REDIS_DB')

SEND_GAS_MAX_FEE_PER_GAS = int(os.environ.get('SEND_GAS_MAX_FEE_PER_GAS'))
SEND_GAS_MAX_PRIORITY_FEE_PER_GAS = int(os.environ.get('SEND_GAS_MAX_PRIORITY_FEE_PER_GAS'))

INNER_DB_NAME = os.environ.get('INNER_DB_NAME')
INNER_DB_USER = os.environ.get('INNER_DB_USER')
INNER_DB_PASSWORD = os.environ.get('INNER_DB_PASSWORD')
INNER_DB_HOST = os.environ.get('INNER_DB_HOST')

OUTER_DB_NAME = os.environ.get('OUTER_DB_NAME')
OUTER_DB_USER = os.environ.get('OUTER_DB_USER')
OUTER_DB_PASSWORD = os.environ.get('OUTER_DB_PASSWORD')
OUTER_DB_HOST = os.environ.get('OUTER_DB_HOST')

logging.basicConfig(level=logging.INFO)
LOG_FILE_MAXSIZE = int(os.environ.get('LOG_FILE_MAXSIZE'))
file_handler = RotatingFileHandler('app_TTP.log', maxBytes=LOG_FILE_MAXSIZE, backupCount=0)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)
logger = logging.getLogger(__name__)

with open(Path(__file__).resolve().parent / "abi" / "erc20_abi.json", "r") as file:
    ERC20_ABI = json.load(file)

app = Flask(__name__)
socketio = SocketIO(app)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY_TTP')
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{INNER_DB_USER}:{INNER_DB_PASSWORD}@{INNER_DB_HOST}/{INNER_DB_NAME}'
app.config['SQLALCHEMY_BINDS'] = {
    'inner_BA_DB': f'postgresql://{INNER_DB_USER}:{INNER_DB_PASSWORD}@{INNER_DB_HOST}/{INNER_DB_NAME}',
    'outer_CENTRAL_DB': f'postgresql://{OUTER_DB_USER}:{OUTER_DB_PASSWORD}@{OUTER_DB_HOST}/{OUTER_DB_NAME}'
}

db = DatabaseSingleton(app).db
celery = Celery(app.import_name, backend=CELERY_RESULT_BACKEND, broker=CELERY_BROKER_URL)
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


@celery.task
def gas_accumulation_task(transaction):
    try:
        web3 = Web3Singleton()
        sys.stdout.write(transaction)

        if 'gas_collected' in transaction and transaction['gas_sended'] == 1:
            return token_transfer_task.s(transaction).apply_async()

        token_contract = web3.eth.contract(address=transaction['to'], abi=ERC20_ABI)
        client_address = transaction['token_data'][0]
        amount = transaction['token_data'][1]

        gas_estimate = token_contract.functions.transfer(
            COLD_WALLET,
            amount
        ).estimate_gas({'from': client_address})
        gas_price_wei = web3.eth.gas_price
        gas_estimate_wei = int(gas_estimate * gas_price_wei)
        nonce = web3.eth.get_transaction_count(GAS_KEEPER)
        send_gas_tx = {
            'from': GAS_KEEPER,
            'to': client_address,
            'value': gas_estimate_wei,
            'nonce': nonce,
            'gas': 21000,
            'maxFeePerGas': SEND_GAS_MAX_FEE_PER_GAS,
            'maxPriorityFeePerGas': SEND_GAS_MAX_PRIORITY_FEE_PER_GAS,
            'chainId': transaction['chainId']
        }

        signed = web3.eth.account.sign_transaction(send_gas_tx, GAS_KEEPER_PK)
        tx_hash = web3.eth.send_raw_transaction(signed.rawTransaction)
        hex_tx_hash = hex(int(tx_hash.hex(), 16))
        logger.info(f'Gas-sending TX: {hex_tx_hash}')
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash)

        if receipt['status'] == 1:
            logger.info(f'Gas-sending TX: {hex_tx_hash} successfully mined.')
            return {'nonce': nonce, 'status': receipt['status']}
        else:
            logger.info('Transaction failed.')

    except ValueError as e:
        error_message = str(e)
        if "insufficient funds for gas * price + value" in error_message:
            logger.info('ALERT! Lack of funds on gas keeper.')
            asyncio.run(send_transaction_to_rabbitmq(transaction, 'Gas_Lack'))
            raise Ignore()
    except Exception as e:
        logging.exception(f'[!] GC unexpected error: {e}')
        asyncio.run(send_transaction_to_rabbitmq(transaction, 'Processing_Failures'))
        raise Ignore()


@celery.task
def accumulation_complete_task(*args):
    gk_nonce = args[0]['nonce']
    key = f'TX:{gk_nonce}'

    while True:
        if redis_client.exists(key):
            redis_client.delete(key)
            logger.info('TX founded in cache!')
            break
        else:
            logger.info('TX yet not founded in cache...')
        time.sleep(5)


@celery.task
def token_transfer_task(*args):
    transaction = args[1]
    try:
        web3 = Web3Singleton()
        required_fields = ['to', 'token_data']
        if not all(field in transaction for field in required_fields):
            raise ValueError(f"TX is missing one or more required fields: {required_fields}")

        token_contract_address = transaction['to']
        client_address = transaction['token_data'][0]
        private_key = get_private_key_for_address(client_address)
        amount = transaction['token_data'][1]

        token_contract = web3.eth.contract(address=token_contract_address, abi=ERC20_ABI)

        token_transfer_tx = token_contract.functions.transfer(
            COLD_WALLET,
            amount
        ).build_transaction({
            'from': client_address,
            'nonce': web3.eth.get_transaction_count(client_address),
        })

        signed_tx = web3.eth.account.sign_transaction(token_transfer_tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logger.info(f'Token-transfer TX HASH: {tx_hash.hex()}')

        receipt = web3.eth.wait_for_transaction_receipt(tx_hash)

        if receipt['status'] == 1:
            logger.info('Token-transfer TX HASH: {tx_hash.hex()} successfully mined.\n\n')
            update_transaction_status_in_db(transaction.get('transaction_id'))
        else:
            logger.info('Token transfer transaction failed.')

        return receipt['status']
    except Exception as e:
        logging.exception(f'[!] ERROR unexpected : {e}')
        transaction['gas_collected'] = '1'

        asyncio.run(send_transaction_to_rabbitmq(transaction, 'Processing_Failures'))


@celery.task
def check_gas_lack_task():
    try:
        web3 = Web3Singleton()

        gas_keeper_balance = web3.eth.get_balance(GAS_KEEPER)
        logger.info(f'Current GK Balance: {gas_keeper_balance}')
        if gas_keeper_balance == 0:
            logger.info("[!] ALARM! Wallet funds are FULLY EXHAUSTED!")
        elif gas_keeper_balance < GAS_KEEPER_ALERT_VALUE:
            logger.info("ALERT! Wallet funds are danger close to exhaustion!")

        previous_balance = int(redis_client.get('eth_gas_keeper_balance') or 0)
        logger.info(f'Previous GK Balance: {previous_balance}')

        if gas_keeper_balance > previous_balance and gas_keeper_balance > GAS_KEEPER_ALERT_VALUE:
            logger.info(f'GK balance changed: {previous_balance} -> {gas_keeper_balance}')
            asyncio.run(rerun_gas_collection('Gas_Lack'))

        redis_client.set('eth_gas_keeper_balance', gas_keeper_balance)
        message = 'Gas_Lack check task completed successfully'
        return message
    except Exception as e:
        logging.exception(f"[!] ERROR in Gas_Lack check task: {e}")
        return f"Task failed: {e}"


celery.conf.beat_schedule = {
    'check_gas_lack_task': {
        'task': 'ModuleTokenTXProcessor.check_gas_lack_task',
        'schedule': crontab(minute=f'*/{CHECK_GAS_LACK_TASK_FREQUENCY}'),
    },
}


def include_object(object, name, type_, reflected, compare_to):
    return name != 'user_wallets'


def get_private_key_for_address(address):
    try:
        with app.app_context():
            user_wallet = db.session.query(UserWallet).filter_by(address=address).first()

        if user_wallet:
            return user_wallet.private_key
        else:
            return None

    except Exception as e:
        logging.exception(f"[!] ERROR with try to get PK: {e}")
        return None


def update_transaction_status_in_db(transaction_id):
    from ModuleBlockAnalyzer.models import TRANSACTION_RCW
    with app.app_context():
        try:
            transaction = db.session.query(TRANSACTION_RCW).get(transaction_id)

            if transaction:
                transaction.state = 'Complete'
                db.session.commit()
                logger.info(f'TX id: {transaction_id} was update in the DB.')
            else:
                logger.info(f'TX id: {transaction_id} not found in the DB.')

        except Exception as e:
            sys.stdout.write(f"Error updating transaction status in the database: {e}")


async def rerun_gas_collection(source_queue_name):
    try:
        connection = await aio_pika.connect_robust(CELERY_BROKER_URL)
        channel = await connection.channel()

        source_queue = await channel.declare_queue(source_queue_name, durable=True)
        message_count = source_queue.declaration_result.message_count
        logger.info(f'QTY of messages in rerun queue: {message_count}')
        if message_count > 0:
            async with source_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        transaction = pickle.loads(message.body)
                        await send_transaction_to_rabbitmq(transaction, 'Gas_Collection')
        logger.info("All messages was ported in Gas Collection queue.")
    except Exception as e:
        logging.exception(f"[!] ERROR with porting messages: {e}")


async def token_transaction_handling(transaction):

    task_chain = chain(
        gas_accumulation_task.s(transaction),
        accumulation_complete_task.s(transaction),
        token_transfer_task.s(transaction)
    )

    task_chain.apply_async()


async def get_token_transactions_from_rabbitmq(app):
    connection = await aio_pika.connect_robust(CELERY_BROKER_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue('Cas_Collection', durable=True)
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                transaction = pickle.loads(message.body)
                logger.info(f'New token-transfer TX recieved.')
                await token_transaction_handling(transaction)


####
# celery -A ModuleTokenTXProcessor.celery beat --loglevel=info
### -P solo вынужденная мера из-за винды, в продакшене потребуется многопоточность
# celery -A ModuleTokenTXProcessor.celery worker --loglevel=info -c 8 -Q celery -P solo