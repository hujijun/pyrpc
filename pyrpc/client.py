import logging
import asyncio
from asyncio import streams
import msgpack

from pyrpc.base import RpcBase, SocketCloseException,ResponseStatus, RpcRequest, RpcResponse


class RpcClient(RpcBase):

    host = port = None

    async def get_response(self) -> RpcResponse:
        """接收帧"""
        channel_id, code, page_size = await self.recv_header()
        body = await self.recv_size(page_size) if page_size else b""
        logging.debug(f"Recv channel_id: {channel_id}, code: {code}, page_size: {page_size}, body: {body}")
        return RpcResponse(channel_id, code, body)

    def send_request(self , request: RpcRequest):
        self.send_frame(request.channel_id, request.method_id, request.args)

    def __init__(self, reader: streams.StreamReader, writer: streams.StreamWriter, max_channel=20):
        super().__init__(reader, writer, set(range(max_channel)))
        self.futures = {}
        # 启动接收服务端信息
        self.recv_loop = asyncio.create_task(self.loop_forever())

    async def reset_connection(self):
        while True:
            try:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
                break
            except Exception as error:
                logging.exception(error)
                await asyncio.sleep(5)

    async def loop_forever(self):
        while not self.closed:
            try:
                response = await self.get_response()
            except SocketCloseException:
                self.closed = True
                # 弹出执行中的抛异常处理
                async with self.lock:
                    while self.futures:
                        self.futures.popitem()[1].set_exception(Exception("Close the connection"))
                    await self.reset_connection()
                    self.closed = False
                continue
            if response.channel_id not in self.futures:
                logging.info(f"未知channel_id={response.channel_id}, status={response.status}, body={response.body}")
                continue
            async with self.lock:
                future = self.futures.pop(response.channel_id)
                self.channel_ids.add(response.channel_id)
            if response.status == ResponseStatus.fail:
                future.set_exception(Exception(response.body))
            future.set_result(msgpack.unpackb(response.body, use_list=False, raw=False) if response.body else None)

    async def async_exec(self, method_id: int, *args):
        async with self.lock:
            channel_id = self.channel_ids.pop()
            self.futures[channel_id] = asyncio.Future()
            self.send_request(RpcRequest(channel_id, method_id, args or b""))
        return await self.futures[channel_id]

    async def close(self):
        self.recv_loop.cancel()
        await super().close()

    @classmethod
    async def create_client(cls, host: str="127.0.0.1", port: int=1000):
        reader, writer = await asyncio.open_connection(host, port)
        cls.host = host
        cls.port = port
        return cls(reader, writer)
