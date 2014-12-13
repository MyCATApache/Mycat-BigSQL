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

import io.netty.channel.ChannelHandlerContext;

import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.mysql.MySQLMessage;
import org.opencloudb.net.ChannelDataHandler;
import org.opencloudb.net.ConnectionInfo;
import org.opencloudb.net.FrontSession;
import org.opencloudb.server.parser.ServerParse;

/**
 * 前端命令处理器
 * 
 * @author mycat
 */
public class FrontendCommandHandler implements ChannelDataHandler {
	private static final Logger LOGGER = Logger
			.getLogger(FrontendCommandHandler.class);

	public static FrontendCommandHandler INSTANCE = new FrontendCommandHandler();

	@Override
	public void handle(ChannelHandlerContext ctx, byte[] data) {

		FrontSession session = NettyUtil.getFrontSession(ctx);
		switch (data[4]) {
		case MySQLPacket.COM_INIT_DB:
			initDB(ctx, data);
			break;
		case MySQLPacket.COM_QUERY:
			queryCmd(session, data);
			break;
		case MySQLPacket.COM_PING:
			ping(session);
			break;
		case MySQLPacket.COM_QUIT:
			LOGGER.info("quit comd ,close connection");
			NettyUtil.getFrontSession(ctx).close("quit cmd");
			break;
		case MySQLPacket.COM_PROCESS_KILL:
			kill(session, data);
			break;
		case MySQLPacket.COM_STMT_PREPARE:
			notSupported(session, data);
			break;
		case MySQLPacket.COM_STMT_EXECUTE:
			notSupported(session, data);
			break;
		case MySQLPacket.COM_STMT_CLOSE:
			notSupported(session, data);
			break;
		case MySQLPacket.COM_HEARTBEAT:
			heartbeat(session, data);
			break;
		default:
			notSupported(session, data);
		}
	}

	public void initDB(ChannelHandlerContext ctx, byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		mm.position(5);
		String db = mm.readString();
		FrontSession session = NettyUtil.getFrontSession(ctx);
		ConnectionInfo conInfo=session.getConInfo();
		String user = conInfo.getUser();
		if (!FrontendAuthenticator.schemaAllowed(ctx, db, user,
				conInfo.getHost())) {
			return;
		}
		conInfo.setSchema(db);
		NettyUtil.writeBytes(ctx, OkPacket.OK);
	}

	public void queryCmd(FrontSession session, byte[] data) {
		ConnectionInfo conInfo = session.getConInfo();
		// 取得语句
		MySQLMessage mm = new MySQLMessage(data);
		mm.position(5);
		String sql = null;
		String charSet = conInfo.getCharset();
		try {
			sql = mm.readString(charSet);
		} catch (UnsupportedEncodingException e) {
			session.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET,
					"Unknown charset '" + charSet + "'");
			return;
		}
		if (sql == null || sql.isEmpty()) {
			session.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND,
					"Empty SQL");
			return;
		}
		// remove last ';'
		if (sql.endsWith(";")) {
			sql = sql.substring(0, sql.length() - 1);
		}

		// 执行查询
		query(session, sql);

	}

	private void ping(FrontSession session) {
		session.writeOK();
	}

	private void heartbeat(FrontSession session, byte[] data) {
		session.writeOK();
	}

	private void kill(FrontSession session, byte[] data) {
		session.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
				"Unknown command");
	}

	private void notSupported(FrontSession session, byte[] data) {
		session.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
				"Not supported yet");
	}

	public void unknown(FrontSession session, byte[] data) {
		session.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
				"Unknown command");
	}

	public void query(FrontSession session, String sql) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(new StringBuilder()
					.append(session.getCtx().channel().remoteAddress())
					.append(sql).toString());
		}
		//
		int rs = ServerParse.parse(sql);
		int sqlType = rs & 0xff;

		switch (sqlType) {
		case ServerParse.EXPLAIN:
			ExplainHandler.handle(sql, session, rs >>> 8);
			break;
		case ServerParse.SET:
			SetHandler.handle(sql, session, rs >>> 8);
			break;
		case ServerParse.SHOW:
			ShowHandler.handle(sql, session, rs >>> 8);
			break;
		case ServerParse.SELECT:
			SelectHandler.handle(sql, session, rs >>> 8);
			break;
		case ServerParse.START:
			StartHandler.handle(sql, session, rs >>> 8);
			break;
		case ServerParse.BEGIN:
			BeginHandler.handle(sql, session);
			break;
		case ServerParse.SAVEPOINT:
			SavepointHandler.handle(sql, session);
			break;
		case ServerParse.KILL:
			KillHandler.handle(sql, rs >>> 8, session);
			break;
		case ServerParse.KILL_QUERY:
			LOGGER.warn(new StringBuilder().append("Unsupported command:")
					.append(sql).toString());
			session.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"Unsupported command");
			break;
		case ServerParse.USE:
			UseHandler.handle(sql, session, rs >>> 8);
			break;
		case ServerParse.COMMIT:
			session.commit();
			break;
		case ServerParse.ROLLBACK:
			session.rollback();
			break;
		case ServerParse.HELP:
			LOGGER.warn(new StringBuilder().append("Unsupported command:")
					.append(sql).toString());
			session.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR,
					"Unsupported command");
			break;
		case ServerParse.MYSQL_CMD_COMMENT:
			session.writeOK();
			break;
		case ServerParse.MYSQL_COMMENT:
			session.writeOK();
			break;
		default:
			if (session.isReadOnly()) {
				LOGGER.warn(new StringBuilder().append("User readonly:")
						.append(sql).toString());
				session.writeErrMessage(ErrorCode.ER_USER_READ_ONLY,
						"User readonly");
				break;
			}
			session.execute(sql, rs & 0xff);
		}
	}

}