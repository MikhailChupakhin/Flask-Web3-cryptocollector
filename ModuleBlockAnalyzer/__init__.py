# ModuleBlockAnalyzer/__init__.py
import json
import logging
import os
import pickle
import traceback

import aio_pika
import redis
import websockets
from dotenv import load_dotenv
from flask import Flask
from flask_socketio import SocketIO
from web3 import Web3
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

from logging.handlers import RotatingFileHandler
from web3.middleware import geth_poa_middleware
from eth_utils import to_checksum_address
from .tokens_adresses import TOKEN_ADDRESSES
from asyncio import Lock

load_dotenv()
db = SQLAlchemy()
migrate = Migrate()

GAS_KEEPER = os.environ.get('GAS_KEEPER')
BLOCK_PACK_PARAMETER = int(os.environ.get('BLOCK_PACK_PARAMETER'))

INNER_DB_NAME = os.environ.get('INNER_DB_NAME')
INNER_DB_USER = os.environ.get('INNER_DB_USER')
INNER_DB_PASSWORD = os.environ.get('INNER_DB_PASSWORD')
INNER_DB_HOST = os.environ.get('INNER_DB_HOST')

OUTER_DB_NAME = os.environ.get('OUTER_DB_NAME')
OUTER_DB_USER = os.environ.get('OUTER_DB_USER')
OUTER_DB_PASSWORD = os.environ.get('OUTER_DB_PASSWORD')
OUTER_DB_HOST = os.environ.get('OUTER_DB_HOST')

SQLALCHEMY_DATABASE_URI = f'postgresql://{INNER_DB_USER}:{INNER_DB_PASSWORD}@{INNER_DB_HOST}/{INNER_DB_NAME}'


REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')
REDIS_DB = os.environ.get('REDIS_DB')


logging.basicConfig(level=logging.INFO)
LOG_FILE_MAXSIZE = int(os.environ.get('LOG_FILE_MAXSIZE'))
file_handler = RotatingFileHandler('app_BA.log', maxBytes=LOG_FILE_MAXSIZE, backupCount=0)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)
logger = logging.getLogger(__name__)

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

blocks_pack_counter = 0
block_processing_lock = Lock()


def include_object(object, name, type_, reflected, compare_to):
    return name != 'user_wallets'


def save_transaction_to_redis(nonce):
    key = f'TX:{nonce}'
    redis_client.set(key, 1)


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY_BA')
    app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
    app.config['SQLALCHEMY_BINDS'] = {
        'inner_BA_DB': f'postgresql://{INNER_DB_USER}:{INNER_DB_PASSWORD}@{INNER_DB_HOST}/{INNER_DB_NAME}',
        'outer_CENTRAL_DB': f'postgresql://{OUTER_DB_USER}:{OUTER_DB_PASSWORD}@{OUTER_DB_HOST}/{OUTER_DB_NAME}'
    }
    db.init_app(app)
    migrate.init_app(app, db, include_object=include_object)
    socketio = SocketIO(app, async_mode='threading')

    from ModuleBlockAnalyzer.models import LAST_PROCECCED_BLOCK_NUMBER, TRANSACTION_RCW

    return app, socketio, db


def create_models(app):
    with app.app_context():
        db.create_all()


def create_components():
    app, socketio, db = create_app()
    web3 = create_web3(os.environ.get('PROVIDER_URL'))

    with app.app_context():
        create_models(app)

    return app, socketio, web3


def create_web3(provider_url):
    web3 = Web3(Web3.HTTPProvider(provider_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return web3


def update_blocks_pack_counter():
    global blocks_pack_counter
    blocks_pack_counter += 1
    if blocks_pack_counter >= BLOCK_PACK_PARAMETER:
        blocks_pack_counter = 0
        return True
    return False


def save_last_processed_block(app, block_number):
    global blocks_pack_counter
    from ModuleBlockAnalyzer.models import LAST_PROCECCED_BLOCK_NUMBER
    with app.app_context():
        last_processed_block = LAST_PROCECCED_BLOCK_NUMBER.query.first()
        if last_processed_block:
            last_processed_block.block_number = block_number
        else:
            new_last_processed_block = LAST_PROCECCED_BLOCK_NUMBER(block_number=block_number)
            db.session.add(new_last_processed_block)

        db.session.commit()
        blocks_pack_counter = 0
        logger.info(f'New number last processed block {block_number} saved, counter reseted!')


def get_last_processed_block_number(app, web3):
    from ModuleBlockAnalyzer.models import LAST_PROCECCED_BLOCK_NUMBER

    with app.app_context():
        last_processed_block = LAST_PROCECCED_BLOCK_NUMBER.query.first()
        if last_processed_block:
            return last_processed_block.block_number
        else:
            latest_block_number = web3.eth.get_block('latest')
            return latest_block_number


async def connect_websocket(app, socketio, web3):
    uri = os.environ.get('URI_BA')
    try:
        async with websockets.connect(uri) as websocket:
            while True:
                last_processed_block_number = get_last_processed_block_number(app, web3)
                logger.info(f'LAST BLOCK IN DB --- {last_processed_block_number}')
                latest_block_info = web3.eth.get_block('latest')
                latest_block_number = latest_block_info['number']
                logger.info(f'LAST BLOCK IN NET --- {latest_block_number}')
                if last_processed_block_number < latest_block_number:
                    start_block = last_processed_block_number + 1
                    end_block = latest_block_number
                    await process_blocks_range(app, socketio, web3, start_block, end_block)
                else:
                    await handle_old_block(app, socketio, web3, latest_block_number)
                    break
            logger.info('+++ Raw blocks processed, subscribing for new blocks!')

            # subscription_request = {
            #     "jsonrpc": "2.0",
            #     "method": "eth_subscribe",
            #     "params": ["newHeads", {
            #         "includeTransactions": True,
            #     }],
            #     "id": 1,
            # }

            # subscription_request = '{"method": "eth_subscribe", "params": ["newHeads"], "id": 1}'

            subscription_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            }

            if websocket.open:
                # await websocket.send(json.dumps(subscription_request))
                await websocket.send(json.dumps(subscription_request))
                logger.info('WSS succesfully connected.')
            else:
                logger.info('[!] WSS is not active!')

            while True:
                response = await websocket.recv()
                data = json.loads(response)
                if "params" in data and "result" in data["params"]:
                    header = data["params"]["result"]

                    await handle_new_block(app, socketio, web3, header)

    except websockets.exceptions.ConnectionClosedError as e:
        logger.info(f"WebSocket connection closed unexpectedly: {e}")

    except websockets.exceptions.WebSocketException as e:
        logger.info(f"WebSocket exception: {e}")
        traceback.print_exc()

    except Exception as e:
        logger.info(f"An error occurred during WebSocket connection: {e}")
        traceback.print_exc()


async def handle_transactions_in_block(app, block_info, target_addresses, block_number):
    eth_transactions = []
    token_transactions = []

    for transaction in block_info['transactions']:
        if transaction['from'] == GAS_KEEPER:
            nonce = transaction['nonce']
            save_transaction_to_redis(nonce)
            logger.info(f'TX was saved to cache.')
            continue

        if transaction.get('input', '').startswith('0xa9059cbb'):
            token_recipient = to_checksum_address('0x' + transaction.get('input')[34:74])
            if token_recipient in target_addresses:
                tx_input = transaction.get('input', '')
                token_value = int(tx_input[74:], 16)
                token_address = transaction.get('to', '')
                if token_address in TOKEN_ADDRESSES:
                    token_symbol = TOKEN_ADDRESSES[token_address]
                    token_data = (token_recipient, token_value, token_symbol)
                    token_transactions.append(token_data)
                    transaction_id = await save_transaction_to_db(app, transaction, block_number, token_data=token_data)
                    await send_transaction_to_rabbitmq(transaction, 'Gas_Collection', transaction_id=transaction_id,
                                                       token_data=token_data)
            continue

        if transaction['to'] in target_addresses:
            eth_transactions.append(transaction)

    if eth_transactions:
        for transaction in eth_transactions:
            transaction_id = await save_transaction_to_db(app, transaction, block_number)
            await send_transaction_to_rabbitmq(transaction, 'Regular_Transactions', transaction_id=transaction_id)

    if eth_transactions or token_transactions:
        save_last_processed_block(app, block_number)

    logger.info(f'Total TX in the block: {len(block_info["transactions"])}')
    logger.info(f'Regular TX to be processed: {len(eth_transactions)}')
    logger.info(f'Token TX to be processed: {len(token_transactions)}')
    logger.info(f'Block counter: {blocks_pack_counter}')


async def process_blocks_range(app, socketio, web3, start_block, end_block):
    for block_number in range(start_block, end_block + 1):
        await handle_old_block(app, socketio, web3, block_number)


async def handle_old_block(app, socketio, web3, block_number):
    target_addresses = get_target_addresses(app)
    block_info = web3.eth.get_block(block_number, full_transactions=True)

    logger.info(f'+++ Processing block: {block_number} +++\n')

    async with block_processing_lock:
        last_processed_block_number = get_last_processed_block_number(app, web3)
        if block_number <= last_processed_block_number:
            logger.info(f'Block {block_number} was processed earlier, skip it...')
            return
        await handle_transactions_in_block(app, block_info, target_addresses, block_number)

        save_last_processed_block(app, block_number)

        socketio.emit('new_block', {'block': block_number}, namespace='/')


def get_target_addresses(app):
    from ModuleBlockAnalyzer.models import UserWallet
    with app.app_context():
        target_addresses = db.session.query(UserWallet.address).distinct().all()
        target_addresses = [row[0] for row in target_addresses]

    return target_addresses


async def save_transaction_to_db(app, transaction, block_number, token_data=None):
    from ModuleBlockAnalyzer.models import TRANSACTION_RCW

    with app.app_context():
        try:
            new_pending_transaction = TRANSACTION_RCW(
                state='Pending',
                block_number=block_number,
                sender_address=transaction['from'],
                receiver_address=transaction['to'],
                value=str(transaction['value']),
                gas=transaction.get('gas', 0),
                gasPrice=transaction.get('gasPrice', 0),
                total_gas_cost=str(transaction.get('gas', 0) * transaction.get('gasPrice', 0)),
                nonce=transaction.get('nonce', 0)
            )

            if token_data:
                recipient, value, symbol = token_data
                new_pending_transaction.is_token_sending = True
                new_pending_transaction.token_recipient = recipient
                new_pending_transaction.token_value = str(value)
                new_pending_transaction.token_symbol = symbol

            db.session.add(new_pending_transaction)
            db.session.commit()

            logger.info('TX was succesfully saved to DB.')
            return new_pending_transaction.id

        except Exception as e:
            logger.info(f'[!] ERROR during saving TX to DB: {str(e)}')


async def send_transaction_to_rabbitmq(transaction, routing_key, transaction_id=None, token_data=None):
    try:
        connection = await aio_pika.connect_robust("amqp://RabbitAdmin:a3fe45fdgj@localhost/localhost")
        channel = await connection.channel()

        transaction_dict = dict(transaction)

        if transaction_id:
            transaction_dict['transaction_id'] = transaction_id
        if token_data:
            transaction_dict['is_token_sending'] = '1'
            transaction_dict['token_data'] = token_data

        body = pickle.dumps(transaction_dict)

        exchange_name = 'amq.direct'
        exchange = await channel.declare_exchange(exchange_name, aio_pika.ExchangeType.DIRECT, durable=True)

        await exchange.publish(
            aio_pika.Message(body=body),
            routing_key=routing_key
        )
        logger.info(f" [x] Sent TX to RabbitMQ with exchange '{exchange_name}' and routing key '{routing_key}'")
    finally:
        if connection:
            await connection.close()


async def handle_new_block(app, socketio, web3, header):
    block_number_hex = header['number']
    block_number = int(block_number_hex, 16)
    block_info = web3.eth.get_block(block_number, full_transactions=True)
    target_addresses = set(get_target_addresses(app))
    logger.info(f'+++ New block recieved: {block_number}\n')

    async with block_processing_lock:
        await handle_transactions_in_block(app, block_info, target_addresses, block_number)

        if update_blocks_pack_counter() == 1:
            save_last_processed_block(app, block_number)

        socketio.emit('new_block', {'block': block_number}, namespace='/')
