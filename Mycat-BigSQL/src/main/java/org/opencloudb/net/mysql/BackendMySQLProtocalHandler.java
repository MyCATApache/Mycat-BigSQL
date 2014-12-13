package org.opencloudb.net.mysql;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;

import org.opencloudb.net.ChannelDataHandler;
import org.opencloudb.net.ConnectionInfo;

public class BackendMySQLProtocalHandler extends ChannelHandlerAdapter {

	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		System.out.println("exceptionCaught " + ctx);
		cause.printStackTrace();

	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		BackMysqlConnection con = (BackMysqlConnection) ctx.channel()
				.attr(AttributeKey.valueOf(NettyUtil.BACK_MYSQL_CON_KEY))
				.getAndRemove();
		NettyUtil.setConnectionHandler(ctx, MySQLBackendAuthenticator.INSTANCE);
		con.setCtx(ctx);
		try {
			con.getConInfo().setAddress(
					(InetSocketAddress) ctx.channel().localAddress(),
					(InetSocketAddress) ctx.channel().remoteAddress());
		} catch (Exception e) {
			e.printStackTrace();
		}
		NettyUtil.setBackendConReq(ctx, con);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		ConnectionInfo conInf = NettyUtil.getConnectionInfo(ctx);
		System.out.println("channelInactive " + conInf);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		// super.channelRead(ctx, msg);
		ChannelDataHandler dataHandler = NettyUtil.getConnectionHandler(ctx);
		dataHandler.handle(ctx, (byte[]) msg);
	}

	@Override
	public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise)
			throws Exception {
		System.out.println("disconnect " + ctx);

	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise promise)
			throws Exception {
		System.out.println("close " + ctx);

	}

}
