import asyncio
from asyncio import streams
import logging
import struct
import msgpack


class SocketCloseException(Exception):
    pass


class Rpc(object):
    # 包头4字节分别1字节channelId 1字节函数Id 2字节包长度，可根据实际情况修改
    packet_header_struct_fmt = "!BBH"
    packet_header_struct = struct.Struct(packet_header_struct_fmt)
    packet_header_size = struct.calcsize(packet_header_struct_fmt)

    # 执行成功
    code_success = 1
    # 执行异常
    code_fail = 0


    def __init__(self, reader: streams.StreamReader, writer: streams.StreamWriter):
        self.reader, self.writer = reader, writer
        self.closed = False

    async def recv_size(self, size) -> bytes:
        """接收指定长度的数据"""
        data = b""
        while not self.closed and size > 0:
            _data = await self.reader.read(size)
            if _data == b'':
                raise SocketCloseException()
            data += _data
            size -= len(_data)
        return data

    async def recv_header(self) -> tuple:
        """接收包头"""
        channel_id, method_number, page_size = self.packet_header_struct.unpack(await self.recv_size(self.packet_header_size))
        return channel_id, method_number, page_size

    async def recv_frame(self) -> tuple:
        """接收1帧数据"""
        channel_id, code, page_size = await self.recv_header()
        body = await self.recv_size(page_size)
        logging.debug(f"Recv channel_id: {channel_id}, code: {code}, page_size: {page_size}, body: {body}")
        return channel_id, code, body

    def send_frame(self, channel_id, code, context):
        """发送1帧数据"""
        if self.closed:
            raise SocketCloseException()
        if not isinstance(context, bytes):
            context = msgpack.packb(context)
        logging.debug(f"Sending frame: channel_id={channel_id}, code={code}, context={context}")
        context = self.packet_header_struct.pack(channel_id, code, len(context))+context
        self.writer.write(context)

    async def close(self):
        self.closed = True
        self.writer.close()
        await self.writer.wait_closed()


class RpcServer(Rpc):
    router_mapping = {}

    async def recv_frame(self):
        """接收一帧数据"""
        channel_id, method_number, page_size = await self.recv_header()
        if page_size > 0:
            args = msgpack.unpackb(await self.recv_size(page_size), use_list=False, raw=False)
        else:
            args = ()
        logging.debug(f"Recv channel_id: {channel_id}, method_number: {method_number}, page_size: {page_size}, args: {args}")
        return channel_id, method_number, args

    async def exec(self, channel_id, method_number, args):
        if method_number not in self.router_mapping:
            self.send_frame(channel_id,0, b"Invalid method")
            return
        try:
            result = await self.router_mapping[method_number](*args)
        except Exception as e:
            self.send_frame(channel_id,self.code_fail, str(e).encode())
            return
        self.send_frame(channel_id, self.code_success, result)

    @classmethod
    async def handle(cls, reader: streams.StreamReader, writer: streams.StreamWriter):
        ip, port = writer.get_extra_info('peername')
        logging.info(f"Access {ip}:{port}")
        rpc_service = cls(reader, writer)
        while not rpc_service.closed:
            try:
                channel_id, method_number, args = await rpc_service.recv_frame()
                await asyncio.create_task(rpc_service.exec(channel_id, method_number, args))
            except SocketCloseException:
                break
            except Exception as error:
                logging.exception(error)
                continue
        logging.info("Close the connection")
        await rpc_service.close()


class RpcClient(Rpc):

    host = port = None

    @classmethod
    async def create_client(cls, host: str="127.0.0.1", port: int=1000):
        reader, writer = await asyncio.open_connection(host, port)
        cls.host = host
        cls.port = port
        return cls(reader, writer)

    def __init__(self, reader: streams.StreamReader, writer: streams.StreamWriter, max_channel=20):
        super().__init__(reader, writer)
        self.channel_numbers = list(range(max_channel))
        self.futures = {}
        self.lock = asyncio.Lock()
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
                channel_id, code, body = await self.recv_frame()
            except SocketCloseException:
                self.closed = True
                # 弹出执行中的抛异常处理
                async with self.lock:
                    while self.futures:
                        self.futures.popitem()[1].set_exception(Exception("Close the connection"))
                    await self.reset_connection()
                    self.closed = False
                continue
            if channel_id not in self.futures:
                logging.info(f"未知channel_id={channel_id}, code={code}, body={body}")
                continue
            async with self.lock:
                future = self.futures.pop(channel_id)
                self.channel_numbers.append(channel_id)
            if code == self.code_fail:
                future.set_exception(Exception(body))
            future.set_result(msgpack.unpackb(body, use_list=False, raw=False) if body else None)

    async def send_and_recv(self, method_id: int, context=b''):
        async with self.lock:
            channel_id = self.channel_numbers.pop()
            self.futures[channel_id] = asyncio.Future()
            self.send_frame(channel_id, method_id, context)
        return await self.futures[channel_id]

    async def async_exec(self, method_id: int, *args):
        async with self.lock:
            channel_id = self.channel_numbers.pop()
            self.futures[channel_id] = asyncio.Future()
            self.send_frame(channel_id, method_id, args or b"")
        return await self.futures[channel_id]


    async def close(self):
        self.recv_loop.cancel()
        await super().close()


class RpcApp(object):


    def __init__(self, routers:list = None):
        self.routers = routers or {}

    def add_route(self, method_id:int, method: callable):
        self.routers[method_id] = method

    def route(self, method_id:int):
        def decorator(func):
            self.add_route(method_id, func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    async def run_tcp_server(self, ip: str, port: int):
        RpcServer.router_mapping = self.routers
        server = await asyncio.start_server(RpcServer.handle, ip, port)
        async with server:
            logging.info(f'Serving on {ip}:{port}')
            logging.debug(f"routers: {self.routers}")
            await server.serve_forever()
