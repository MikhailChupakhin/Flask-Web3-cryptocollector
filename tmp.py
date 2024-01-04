import websockets
import asyncio
import json

async def connect_websocket():
    uri = 'wss://bsc-mainnet.blastapi.io/b2c68354-8220-4e31-865d-d6e951b46896'
    try:
        async with websockets.connect(uri) as websocket:
            print('Connected successfully')

            # Отправляем запрос на подписку на новые блоки
            subscription_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            }
            await websocket.send(json.dumps(subscription_request))

            # Асинхронный цикл для прослушивания новых блоков
            async for message in websocket:
                print(f"Received message: {message}")

                data = json.loads(message)
                if "params" in data and "result" in data["params"]:
                    header = data["params"]["result"]
                    print(f"New block header received: {header}")

    except websockets.exceptions.ConnectionClosedError as e:
        print(f"WebSocket connection closed unexpectedly: {e}")
    except Exception as e:
        print(f"An error occurred during WebSocket connection: {e}")

# Запускаем асинхронный цикл
asyncio.get_event_loop().run_until_complete(connect_websocket())