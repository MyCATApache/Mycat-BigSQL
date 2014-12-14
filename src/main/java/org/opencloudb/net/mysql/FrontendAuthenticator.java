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

import java.security.NoSuchAlgorithmException;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.MycatSystem;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.mysql.SecurityUtil;
import org.opencloudb.net.ChannelDataHandler;
import org.opencloudb.net.ConnectionInfo;
import org.opencloudb.net.FrontSession;
import org.opencloudb.net.handler.FrontendPrivileges;

/**
 * 前端认证处理器
 * 
 * @author mycat
 */
public class FrontendAuthenticator implements ChannelDataHandler {

	public static FrontendAuthenticator INSTANCE = new FrontendAuthenticator();
	private static final Logger LOGGER = Logger
			.getLogger(FrontendAuthenticator.class);
	private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2,
			0, 0, 0 };

	public void handle(ChannelHandlerContext ctx, byte[] data) {
		// check quit packet
		if (data.length == QuitPacket.QUIT.length
				&& data[4] == MySQLPacket.COM_QUIT) {
			LOGGER.info("quick commmand recieved ,close ");
			ctx.close();
			return;
		}

		AuthPacket auth = new AuthPacket();
		auth.read(data);

		// check user
		// if (!checkUser(auth.user, source.getHost())) {
		// failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '"
		// + auth.user + "'");
		// return;
		// }

		// check password
		if (!checkPassword(ctx, auth.password, auth.user)) {
			failure(ctx, ErrorCode.ER_ACCESS_DENIED_ERROR,
					"Access denied for user '" + auth.user + "'");
			return;
		}

		// check schema
		if (schemaAllowed(ctx, auth.database, auth.user, NettyUtil
				.getConnectionInfo(ctx).getHost())) {
			success(ctx, auth);
		}

	}

	public static boolean checkUser(String user, String host) {
		return MycatSystem.getInstance().getPrivileges().userExists(user, host);
	}

	public static boolean schemaAllowed(ChannelHandlerContext ctx,
			String schema, String user, String userHost) {
		if (!MycatSystem.getInstance().getPrivileges()
				.userExists(user, userHost)) {
			failure(ctx, ErrorCode.ER_DBACCESS_DENIED_ERROR,
					"Access denied for user '" + user + "'" + " from host:"
							+ userHost);
			return false;
		}
		switch (checkSchema(schema, user)) {
		case ErrorCode.ER_BAD_DB_ERROR:
			failure(ctx, ErrorCode.ER_BAD_DB_ERROR, "Unknown database '"
					+ schema + "'");
			return false;
		case ErrorCode.ER_DBACCESS_DENIED_ERROR:
			String s = "Access denied for user '" + user + "' to database '"
					+ schema + "'";
			failure(ctx, ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
			return false;
		default:
			return true;
		}
	}

	protected boolean checkPassword(ChannelHandlerContext ctx, byte[] password,
			String user) {

		String pass = MycatSystem.getInstance().getPrivileges()
				.getPassword(user);

		// check null
		if (pass == null || pass.length() == 0) {
			if (password == null || password.length == 0) {
				return true;
			} else {
				return false;
			}
		}
		if (password == null || password.length == 0) {
			return false;
		}

		// encrypt
		byte[] encryptPass = null;
		try {
			ConnectionInfo conInf = NettyUtil.getConnectionInfo(ctx);
			encryptPass = SecurityUtil.scramble411(pass.getBytes(),
					conInf.getSeed());
		} catch (NoSuchAlgorithmException e) {
			LOGGER.warn(ctx.channel() + " err:", e);
			return false;
		}
		if (encryptPass != null && (encryptPass.length == password.length)) {
			int i = encryptPass.length;
			while (i-- != 0) {
				if (encryptPass[i] != password[i]) {
					return false;
				}
			}
		} else {
			return false;
		}

		return true;
	}

	private static int checkSchema(String schema, String user) {
		if (schema == null) {
			return 0;
		}
		FrontendPrivileges privileges = MycatSystem.getInstance()
				.getPrivileges();
		if (!privileges.schemaExists(schema)) {
			return ErrorCode.ER_BAD_DB_ERROR;
		}
		Set<String> schemas = privileges.getUserSchemas(user);
		if (schemas == null || schemas.size() == 0 || schemas.contains(schema)) {
			return 0;
		} else {
			return ErrorCode.ER_DBACCESS_DENIED_ERROR;
		}
	}

	protected void success(ChannelHandlerContext ctx, AuthPacket auth) {
		ConnectionInfo conInf = NettyUtil.getConnectionInfo(ctx);
		conInf.setUser(auth.user);
		conInf.setSchema(auth.database);
		conInf.setCharsetIndex(auth.charsetIndex);
		// conInf.setHandler(new FrontendCommandHandler(source));
		if (LOGGER.isInfoEnabled()) {
			StringBuilder s = new StringBuilder();
			s.append(ctx.channel()).append('\'').append(auth.user)
					.append("' login success");
			byte[] extra = auth.extra;
			if (extra != null && extra.length > 0) {
				s.append(",extra:").append(new String(extra));
			}
			LOGGER.info(s.toString());
		}

		ctx.writeAndFlush(ctx.alloc().ioBuffer(AUTH_OK.length)
				.writeBytes(AUTH_OK));
		FrontSession session = new FrontSession(ctx, MycatSystem.getInstance()
				.getPrivileges().isReadOnly(conInf.getUser()), conInf);
		NettyUtil.removeConnectionInfo(ctx);
		NettyUtil.updateFrontSession(ctx, session);
		// to query hanlder
		NettyUtil.setConnectionHandler(ctx, FrontendCommandHandler.INSTANCE);
		ctx.read();
	}

	public static void failure(ChannelHandlerContext ctx, int errno, String info) {
		LOGGER.error(ctx.channel().toString() + info);
		NettyUtil.writeErrMessage(ctx, errno, info);
	}

}