package org.opencloudb.net.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class MySQLProtocalDecoder extends ByteToMessageDecoder {
	private final int packetHeaderSize = 4;
	private final int maxPacketSize = 16 * 1024 * 1024;

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf buffer,
			List<Object> out) throws Exception {
		//System.out.println("decode buffer ,read len " + buffer.readableBytes());
		int maxLenth, length = 0;
		for (;;) {
			maxLenth = buffer.readableBytes();
			if (maxLenth < packetHeaderSize) {
				return;
			}
			length = getPacketLength(buffer, buffer.readerIndex());
            if(length>maxPacketSize)
            {
            	throw new IllegalArgumentException(
						"Packet size over the limit "+maxPacketSize);
            }
            else if (length <= maxLenth) {
				byte[] data = new byte[length];
				buffer.readBytes(data);
				// handle(data);
				//System.out.println("segment :len " + data.length);
				out.add(data);
			} else {
				// next read event
				break;
			}
		}
		buffer.discardReadBytes();
	}

	protected int getPacketLength(ByteBuf buffer, int offset) {

		int length = buffer.getByte(offset) & 0xff;
		length |= (buffer.getByte(++offset) & 0xff) << 8;
		length |= (buffer.getByte(++offset) & 0xff) << 16;
		return length + packetHeaderSize;

	}

	private void checkReadBuffer(ByteBuf buffer, int offset, int position) {
		if (offset == 0) {
			if (buffer.capacity() >= maxPacketSize) {
				throw new IllegalArgumentException(
						"Packet size over the limit.");
			}

		}
	}
}
