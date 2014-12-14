package org.opencloudb.net;

import io.netty.channel.ChannelHandlerContext;

public interface ChannelDataHandler {

	void handle(ChannelHandlerContext ctx, byte[] data);
}
