import asyncio
from asyncio import streams
import logging
import struct
from enum import IntEnum

import msgpack


class SocketCloseException(Exception):
    pass

class ResponseStatus(IntEnum):
    success = 1
    fail = 0

class RpcResponse(object):
    channel_id: int
    status: ResponseStatus
    body: bytes

    def __init__(self, channel_id, status, body):
        self.channel_id = channel_id
        self.status = status
        self.body = body

class RpcRequest(object):
    channel_id: int
    method_id: int
    args: tuple

    def __init__(self, channel_id, method_id, args):
        self.channel_id = channel_id
        self.method_id = method_id
        self.args = args


class RpcBase(object):
    # 包头4字节分别1字节channelId 1字节函数Id 2字节包长度，可根据实际情况修改
    packet_header_struct_fmt = "!BBH"
    packet_header_struct = struct.Struct(packet_header_struct_fmt)
    packet_header_size = struct.calcsize(packet_header_struct_fmt)

    def __init__(self, reader: streams.StreamReader, writer: streams.StreamWriter, channel_ids=None):
        self.reader, self.writer = reader, writer
        self.closed = False
        self.lock = asyncio.Lock()
        self.channel_ids = channel_ids or set()

    async def recv_size(self, size, data=b"") -> bytes:
        """接收指定长度的数据"""
        data += await self.reader.read(size)
        if len(data) == size:
            return data
        if data == b'':
            raise SocketCloseException()
        return await self.recv_size(size-len(data), data)

    async def recv_header(self) -> tuple:
        """接收帧"""
        # 接收header
        header_body = await self.recv_size(self.packet_header_size)
        # 解包获取对应的管道ID、code、body长度
        channel_id, code, page_size = self.packet_header_struct.unpack(header_body)
        return channel_id, code, page_size

    def send_frame(self, channel_id, code, context):
        """发送帧"""
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
