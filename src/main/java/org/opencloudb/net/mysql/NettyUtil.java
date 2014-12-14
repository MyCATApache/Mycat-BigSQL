package org.opencloudb.net.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

import java.io.UnsupportedEncodingException;

import org.opencloudb.net.ChannelDataHandler;
import org.opencloudb.net.ConnectionInfo;
import org.opencloudb.net.FrontSession;

public class NettyUtil {
	public static final String CON_INFO_KEY = "conInfo_key";
	public static final String FRONT_SESSION_KEY = "session_key";
	public static final String BACK_MYSQL_CON_KEY = "mysqlcon_key";
	public static final String CON_DATAHANDLER_KEY = "conDataHandler_key";

	public static ConnectionInfo getConnectionInfo(ChannelHandlerContext ctx) {
		return (ConnectionInfo) ctx.attr(AttributeKey.valueOf(CON_INFO_KEY))
				.get();
	}

	public static void removeConnectionInfo(ChannelHandlerContext ctx) {
		ctx.attr(AttributeKey.valueOf(CON_INFO_KEY)).remove();
	}

	public static FrontSession getFrontSession(ChannelHandlerContext ctx) {
		return (FrontSession) ctx.attr(AttributeKey.valueOf(FRONT_SESSION_KEY))
				.get();
	}
	public static void setConnectionInfo(ChannelHandlerContext ctx,
			ConnectionInfo conInfo) {
		ctx.attr(AttributeKey.valueOf(CON_INFO_KEY)).set(conInfo);
	}

	public static ChannelDataHandler getConnectionHandler(
			ChannelHandlerContext ctx) {
		return (ChannelDataHandler) ctx.attr(
				AttributeKey.valueOf(CON_DATAHANDLER_KEY)).get();
	}

	public static BackMysqlConnection getBackMysqlConnection(
			ChannelHandlerContext ctx) {
		return (BackMysqlConnection) ctx.attr(
				AttributeKey.valueOf(BACK_MYSQL_CON_KEY)).get();
	}

	public static void setBackendConReq(ChannelHandlerContext ctx,
			BackMysqlConnection mysqlCon) {

		ctx.attr(AttributeKey.valueOf(BACK_MYSQL_CON_KEY)).set(mysqlCon);
	}

	public static void setConnectionHandler(ChannelHandlerContext ctx,
			ChannelDataHandler dataHandler) {
		ctx.attr(AttributeKey.valueOf(CON_DATAHANDLER_KEY)).set(dataHandler);
	}

	public static void updateFrontSession(ChannelHandlerContext ctx,
			FrontSession frontSession) {
		ctx.attr(AttributeKey.valueOf(FRONT_SESSION_KEY)).set(frontSession);
	}

	
	public static void writeErrMessage(ChannelHandlerContext ctx, int errno,
			String msg) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = 1;
		err.errno = errno;
		err.message = encodeString(msg, "utf-8");
		ByteBuf buf = ctx.alloc().ioBuffer(err.calcPacketSize() + 4);
		err.write(buf);
		ctx.writeAndFlush(buf);
	}

	public final static byte[] encodeString(String src, String charset) {
		if (src == null) {
			return null;
		}
		if (charset == null) {
			return src.getBytes();
		}
		try {
			return src.getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			return src.getBytes();
		}
	}

	/**
	 * write bytes and flush to socket
	 * 
	 * @param ctx
	 * @param byts
	 */
	public final static void writeBytes(ChannelHandlerContext ctx, byte[] byts) {
		ctx.writeAndFlush(
				ctx.alloc().ioBuffer(byts.length).writeBytes(byts));
	}

	public final static void writeBytesNoFlush(ChannelHandlerContext ctx,
			byte[] byts) {
		ctx.write(ctx.alloc().ioBuffer(byts.length).writeBytes(byts));
	}
}
