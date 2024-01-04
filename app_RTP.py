import asyncio

from ModuleRegularTXProcessor import get_regular_transactions_from_rabbitmq, app, web3

if __name__ == "__main__":
    asyncio.run(get_regular_transactions_from_rabbitmq(app, web3))
