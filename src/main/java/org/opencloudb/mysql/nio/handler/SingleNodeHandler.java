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
package org.opencloudb.mysql.nio.handler;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatSystem;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.ConnectionMeta;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.net.ConnectionInfo;
import org.opencloudb.net.FrontSession;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.util.StringUtil;

/**
 * @author mycat
 */
public class SingleNodeHandler implements ResponseHandler, Terminatable {
	private static final Logger LOGGER = Logger
			.getLogger(SingleNodeHandler.class);
	private final RouteResultsetNode node;
	private final RouteResultset rrs;
	private final FrontSession session;
	// only one thread access at one time no need lock
	private volatile byte packetId;
	private volatile boolean isRunning;
	private Runnable terminateCallBack;

	public SingleNodeHandler(RouteResultset rrs, FrontSession session) {
		this.rrs = rrs;
		this.node = rrs.getNodes()[0];
		if (node == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
		if (session == null) {
			throw new IllegalArgumentException("session is null!");
		}
		this.session = session;
	}

	@Override
	public void terminate(Runnable callback) {
		boolean zeroReached = false;

		if (isRunning) {
			terminateCallBack = callback;
		} else {
			zeroReached = true;
		}

		if (zeroReached) {
			callback.run();
		}
	}

	private void endRunning() {
		Runnable callback = null;
		if (isRunning) {
			isRunning = false;
			callback = terminateCallBack;
			terminateCallBack = null;
		}

		if (callback != null) {
			callback.run();
		}
	}

	public void execute() throws Exception {
		ConnectionInfo conINfo = session.getConInfo();
		this.isRunning = true;
		this.packetId = 0;
		final BackendConnection conn = session.getTarget(node);
		if (session.tryExistsCon(conn, node)) {
			_execute(conn);
		} else {
			// create new connection

			MycatConfig conf = MycatSystem.getInstance().getConfig();
			PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
			ConnectionMeta conMeta = new ConnectionMeta(dn.getDatabase(),
					conINfo.getCharset(), conINfo.getCharsetIndex(),
					session.isAutocommit());
			dn.getConnection(conMeta, node, this, node);
		}

	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		session.bindConnection(node, conn);
		_execute(conn);

	}

	private void _execute(BackendConnection conn) {
		if (session.closed()) {
			endRunning();
			session.clearResources(true);
			return;
		}
		conn.setResponseHandler(this);
		try {
			conn.execute(node, session, session.isAutocommit());
		} catch (IOException e1) {
			executeException(conn, e1);
			return;
		}
	}

	private void executeException(BackendConnection c, Throwable e) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_YES;
		err.message = StringUtil.encode("backend err " + e.toString(), session
				.getConInfo().getCharset());

		this.backConnectionErr(err, c);
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		endRunning();
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_YES;
		err.message = StringUtil.encode(e.getMessage(), session.getConInfo()
				.getCharset());
		session.writeErrMessage(ErrorCode.ER_YES, e.getMessage());

	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		err.packetId = ++packetId;
		backConnectionErr(err, conn);

	}

	private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn) {
		endRunning();
		String errmgs = " errno:" + errPkg.errno + " "
				+ new String(errPkg.message);
		LOGGER.warn("execute  sql err :" + errmgs + " con:" + conn);
		session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
		session.setTxInterrupt(errmgs);
		session.writeErrorPkg(errPkg);

	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExcute();
		if (executeResponse) {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),
					false);
			endRunning();
			OkPacket ok = new OkPacket();
			ok.read(data);
			ok.packetId = ++packetId;
			session.setLastInsertId(ok.insertId);
			session.writeOK(ok);

		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {

		// conn.recordSql(source.getHost(), source.getSchema(),
		// node.getStatement());

		// 判断是调用存储过程的话不能在这里释放链接
		if (!rrs.isCallStatement()) {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),
					false);
			endRunning();
		}

		eof[3] = ++packetId;
		session.writeBytes(eof);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		header[3] = ++packetId;
		ByteBuf buf = session.allocate(128, 1024);
		buf.writeBytes(header);
		for (int i = 0, len = fields.size(); i < len; ++i) {
			byte[] field = fields.get(i);
			field[3] = ++packetId;
			buf.writeBytes(field);
		}
		eof[3] = ++packetId;
		buf.writeBytes(eof);
		session.write(buf);
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		row[3] = ++packetId;
		session.writeNoFlush(row);
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_YES;
		err.message = StringUtil.encode(reason, session.getConInfo()
				.getCharset());
		this.backConnectionErr(err, conn);

	}

	public void clearResources() {

	}

	@Override
	public String toString() {
		return "SingleNodeHandler [node=" + node + ", packetId=" + packetId
				+ "]";
	}

}