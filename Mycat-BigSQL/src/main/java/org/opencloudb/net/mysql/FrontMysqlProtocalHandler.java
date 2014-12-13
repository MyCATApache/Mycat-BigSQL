package org.opencloudb.net.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.MycatSystem;
import org.opencloudb.config.Capabilities;
import org.opencloudb.config.Versions;
import org.opencloudb.net.ConnectionInfo;
import org.opencloudb.util.RandomUtil;

public class FrontMysqlProtocalHandler extends ChannelHandlerAdapter {
	private AtomicLong idGen = new AtomicLong(0);

	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {

		System.out.println("exceptionCaught " + ctx);
		cause.printStackTrace();
	}

	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// 生成认证数据
		byte[] rand1 = RandomUtil.randomBytes(8);
		byte[] rand2 = RandomUtil.randomBytes(12);

		// 保存认证数据
		byte[] seed = new byte[rand1.length + rand2.length];
		System.arraycopy(rand1, 0, seed, 0, rand1.length);
		System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
		ConnectionInfo conInf = new ConnectionInfo();
		MycatSystem.getInstance().getConfig().getSystem()
				.setConnectionParams(conInf);
		conInf.setAddress((InetSocketAddress) ctx.channel().remoteAddress(),
				(InetSocketAddress) ctx.channel().localAddress());
		conInf.setSeed(seed);
		NettyUtil.setConnectionInfo(ctx, conInf);

		// 发送握手数据包
		HandshakePacket hs = new HandshakePacket();
		hs.packetId = 0;
		hs.protocolVersion = Versions.PROTOCOL_VERSION;
		hs.serverVersion = Versions.SERVER_VERSION;
		hs.threadId = idGen.incrementAndGet();
		hs.seed = rand1;
		hs.serverCapabilities = getServerCapabilities();
		hs.serverCharsetIndex = (byte) (conInf.getCharsetIndex() & 0xff);
		hs.serverStatus = 2;
		hs.restOfScrambleBuff = rand2;

		ByteBuf buf = ctx.alloc().ioBuffer(hs.calcPacketSize(), 1024);
		hs.writeBuf(buf);
		ctx.writeAndFlush(buf);
		NettyUtil.setConnectionHandler(ctx, FrontendAuthenticator.INSTANCE);
		ctx.read();

	}

	protected int getServerCapabilities() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		// flag |= Capabilities.CLIENT_COMPRESS;
		flag |= Capabilities.CLIENT_ODBC;
		// flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= ServerDefs.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		return flag;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		byte[] mysqlData = (byte[]) msg;
		// System.out.println("channelRead " + ctx + " " + new
		// String(mysqlData));
		NettyUtil.getConnectionHandler(ctx).handle(ctx, mysqlData);

	}


	@Override
	public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise)
			throws Exception {
		super.disconnect(ctx, promise);
		System.out.println("disconnect " + ctx);
	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise promise)
			throws Exception {
		super.close(ctx, promise);
		System.out.println("close " + ctx);

	}

}
