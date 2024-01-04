import asyncio
import logging
import os
import pickle
from logging.handlers import RotatingFileHandler

import aio_pika
import requests
from dotenv import load_dotenv
from flask import Flask

from ModuleRegularTXProcessor.singltones import DatabaseSingleton, Web3Singleton

load_dotenv()

INNER_DB_NAME = os.environ.get('INNER_DB_NAME')
INNER_DB_USER = os.environ.get('INNER_DB_USER')
INNER_DB_PASSWORD = os.environ.get('INNER_DB_PASSWORD')
INNER_DB_HOST = os.environ.get('INNER_DB_HOST')

OUTER_DB_NAME = os.environ.get('OUTER_DB_NAME')
OUTER_DB_USER = os.environ.get('OUTER_DB_USER')
OUTER_DB_PASSWORD = os.environ.get('OUTER_DB_PASSWORD')
OUTER_DB_HOST = os.environ.get('OUTER_DB_HOST')

COLD_WALLET = os.environ.get('COLD_WALLET')
SLEEP_RETRY_503 = float(os.environ.get('SLEEP_RETRY_503'))

logging.basicConfig(level=logging.INFO)
LOG_FILE_MAXSIZE = int(os.environ.get('LOG_FILE_MAXSIZE'))
file_handler = RotatingFileHandler('app_RTP.log', maxBytes=LOG_FILE_MAXSIZE, backupCount=0)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)
logger = logging.getLogger(__name__)


app = Flask(__name__)

app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY_RTP')
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{INNER_DB_USER}:{INNER_DB_PASSWORD}@{INNER_DB_HOST}/{INNER_DB_NAME}'
app.config['SQLALCHEMY_BINDS'] = {
     'inner_BA_DB': f'postgresql://{INNER_DB_USER}:{INNER_DB_PASSWORD}@{INNER_DB_HOST}/{INNER_DB_NAME}',
     'outer_CENTRAL_DB': f'postgresql://{OUTER_DB_USER}:{OUTER_DB_PASSWORD}@{OUTER_DB_HOST}/{OUTER_DB_NAME}'
}

db = DatabaseSingleton(app).db
web3 = Web3Singleton()


def include_object(object, name, type_, reflected, compare_to):
    return name != 'user_wallets'


def get_private_key_for_address(app, db, address):
    from ModuleBlockAnalyzer.models import UserWallet
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


async def regular_transaction_handling(app, web3, transaction):
    logger.info(f'PROCESSING REG TX...')
    try:
        receiver_address = transaction['to']
        private_key = get_private_key_for_address(app, db, receiver_address)

        if private_key:
            gas_used = transaction['gas']
            gas_price = transaction['gasPrice']
            total_gas_cost = gas_used * gas_price
            logger.info(f'Total Gas Cost: {total_gas_cost} Wei')
            retry_count = 0
            tx_hash = None
            while tx_hash == None:
                try:
                    nonce = web3.eth.get_transaction_count(receiver_address)
                    deposit_amount = transaction['value']

                    transaction = {
                        'from': receiver_address,
                        'to': COLD_WALLET,
                        'gas': 21000,
                        'gasPrice': gas_price,
                        'value': deposit_amount - total_gas_cost,
                        'nonce': nonce,
                        'chainId': transaction['chainId']
                    }
                    signed = web3.eth.account.sign_transaction(transaction, private_key)
                    tx_hash = web3.eth.send_raw_transaction(signed.rawTransaction)

                    if tx_hash:
                        logger.info(f'Deposit sent to COLD WALLET. TX Hash: {tx_hash.hex()}')
                        break
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 503:
                        retry_count += 1
                        logger.info(f'503 Response! Retrying {retry_count} after {SLEEP_RETRY_503} sec...')
                        await asyncio.sleep({SLEEP_RETRY_503})
                    else:
                        logger.info(f'Unexpected error: {e}')
                        break
        else:
            logger.info(f'PK for {receiver_address} not founded!')
    except Exception as e:
        logger.info(f"[!!!] ERROR with processing TX: {e}")


async def get_regular_transactions_from_rabbitmq(app, web3):
    rabbitmq_username = os.getenv("RABBITMQ_USERNAME")
    rabbitmq_password = os.getenv("RABBITMQ_PASSWORD")
    rabbitmq_host = os.getenv("RABBITMQ_HOST")
    rabbitmq_port = os.getenv("RABBITMQ_PORT")
    rabbitmq_vhost = os.getenv("RABBITMQ_VHOST")

    rabbitmq_url = f'amqp://{rabbitmq_username}:{rabbitmq_password}@{rabbitmq_host}:{rabbitmq_port}/{rabbitmq_vhost}'

    connection = await aio_pika.connect_robust(rabbitmq_url)
    channel = await connection.channel()
    queue = await channel.declare_queue('Regular_Transactions', durable=True)
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                transaction = pickle.loads(message.body)
                await regular_transaction_handling(app, web3, transaction)

