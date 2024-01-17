import asyncio
import websockets

async def connect_to_server():
    uri = "wss://34.92.14.160:7855/wss/tcp/MzQuOTIuMTQuMTYwOjcyMjQ="  # 将 "your_server_address" 替换为实际的WebSocket服务器地址

    async with websockets.connect(uri) as websocket:
        while True:
            #message = await websocket.recv()  # 接收消息
            #print(f"Received message: {message}")
            # 十六进制字符串
            hex_string = "00f1001500000038009897b838320000323032332d30372d3132323032332d31302d31320000000000000000000000000000000000140000"

            # 将十六进制字符串转换为二进制数据
            binary_data = bytes.fromhex(hex_string)

            # 发送二进制数据
            await websocket.send(binary_data)
            #message = await websocket.recv()  # 接收消息
            #print(f"Received message: {message}")

# 运行连接函数
asyncio.get_event_loop().run_until_complete(connect_to_server())
