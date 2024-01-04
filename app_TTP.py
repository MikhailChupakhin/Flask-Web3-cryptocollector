# main file app_TTP.py
import asyncio

from ModuleTokenTXProcessor import get_token_transactions_from_rabbitmq, app

if __name__ == "__main__":
    asyncio.run(get_token_transactions_from_rabbitmq(app))