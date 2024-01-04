import asyncio
from ModuleBlockAnalyzer import create_components, connect_websocket, logger

app, socketio, web3 = create_components()


async def run_asyncio_task():
    while True:
        try:
            await connect_websocket(app, socketio, web3)
        except Exception as e:
            logger.info(f"An error occurred in the asyncio task: {e}")


if __name__ == "__main__":
    asyncio.run(run_asyncio_task())
    socketio.run(app, debug=True)
