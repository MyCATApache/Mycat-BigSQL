/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.net.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.security.NoSuchAlgorithmException;

import org.opencloudb.config.Capabilities;
import org.opencloudb.mysql.CharsetUtil;
import org.opencloudb.mysql.SecurityUtil;
import org.opencloudb.net.ChannelDataHandler;
import org.opencloudb.net.ConnectionException;

/**
 * MySQL 验证处理器
 * 
 * @author mycat
 */
public class MySQLBackendAuthenticator implements ChannelDataHandler {
	
	public static MySQLBackendAuthenticator INSTANCE=new MySQLBackendAuthenticator();

	private static final long clientFlags = initClientFlags();

	private static long initClientFlags() {
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
		// flag |= Capabilities.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		// client extension
		// flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
		flag |= Capabilities.CLIENT_MULTI_RESULTS;
		return flag;
	}

	private static byte[] passwd(String pass, HandshakePacket hs)
			throws NoSuchAlgorithmException {
		if (pass == null || pass.length() == 0) {
			return null;
		}
		byte[] passwd = pass.getBytes();
		int sl1 = hs.seed.length;
		int sl2 = hs.restOfScrambleBuff.length;
		byte[] seed = new byte[sl1 + sl2];
		System.arraycopy(hs.seed, 0, seed, 0, sl1);
		System.arraycopy(hs.restOfScrambleBuff, 0, seed, sl1, sl2);
		return SecurityUtil.scramble411(passwd, seed);
	}

	private void authenticate(ChannelHandlerContext ctx, BackMysqlConnection con) {
		AuthPacket packet = new AuthPacket();
		packet.packetId = 1;
		packet.clientFlags = clientFlags;
		packet.maxPacketSize = 1024 * 1024 * 16;
		packet.charsetIndex = 0;
		packet.user = con.getUser();
		try {
			packet.password = passwd(con.getPassword(), con.getHandshake());
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
		packet.database = con.getSchema();

		ByteBuf buf = ctx.alloc().ioBuffer(128, 1024);
		packet.write(buf);
		ctx.writeAndFlush(buf);

	}

	@Override
	public void handle(ChannelHandlerContext ctx, byte[] data) {
		BackMysqlConnection con =NettyUtil.getBackMysqlConnection(ctx) ;
		switch (data[4]) {
		case OkPacket.FIELD_COUNT:
			HandshakePacket packet = con.getHandshake();
			if (packet == null) {
				processHandShakePacket(con, data);
				// 发送认证数据包
				authenticate(ctx, con);
				break;
			}
			// 处理认证结果
			con.setAuthenticated(true);
			//create back mysql handle to process data flow
			NettyUtil.setConnectionHandler(ctx, new BackMySQLHandler(con));
			//notify connection required
			con.getRespHandler().connectionAcquired(con);
			break;
		case ErrorPacket.FIELD_COUNT:
			ErrorPacket err = new ErrorPacket();
			err.read(data);
			String errMsg = new String(err.message);
			con.close(errMsg);
			throw new ConnectionException(err.errno, errMsg);

		case EOFPacket.FIELD_COUNT:
			auth323(ctx,con,data[3]);
			break;
		default:
			packet = con.getHandshake();
			if (packet == null) {
				processHandShakePacket(con, data);
				// 发送认证数据包
				authenticate(ctx, con);
				break;
			} else {
				throw new RuntimeException("Unknown Packet!");
			}

		}

	}

	private void processHandShakePacket(BackMysqlConnection con, byte[] data) {
		HandshakePacket packet;
		// 设置握手数据包
		packet = new HandshakePacket();
		packet.read(data);
		con.setHandshake(packet);
		con.setThreadId(packet.threadId);

		// 设置字符集编码
		int charsetIndex = (packet.serverCharsetIndex & 0xff);
		String charset = CharsetUtil.getCharset(charsetIndex);
		boolean charsetValid = false;
		if (charset != null) {
			charsetValid = con.getConInfo().setCharset(charset);
		}
		if (!charsetValid) {
			throw new RuntimeException("Unknown charsetIndex:" + charsetIndex);
		}
	}

	private void auth323(ChannelHandlerContext ctx, BackMysqlConnection con,
			byte packetId) {
		// 发送323响应认证数据包
		Reply323Packet r323 = new Reply323Packet();
		r323.packetId = ++packetId;
		String pass = con.getPassword();
		if (pass != null && pass.length() > 0) {
			byte[] seed = con.getHandshake().seed;
			r323.seed = SecurityUtil.scramble323(pass, new String(seed))
					.getBytes();
		}
		ByteBuf buffer = ctx.alloc().ioBuffer(128, 512);
		r323.write(buffer);
		ctx.writeAndFlush(buffer);
	}

}