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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.opencloudb.mysql.ByteUtil;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.ChannelDataHandler;

/**
 * life cycle: from connection establish to close <br/>
 * every connection has one
 * 
 * @author mycat
 */
public class BackMySQLHandler implements ChannelDataHandler {
	private static final Logger logger = Logger
			.getLogger(BackMySQLHandler.class);
	private static final int RESULT_STATUS_INIT = 0;
	private static final int RESULT_STATUS_HEADER = 1;
	private static final int RESULT_STATUS_FIELD_EOF = 2;

	private volatile int resultStatus;
	private volatile byte[] header;
	private volatile List<byte[]> fields;
	private final BackMysqlConnection con;

	public BackMySQLHandler(BackMysqlConnection con) {
		super();
		this.con = con;
	}

	@Override
	public void handle(ChannelHandlerContext ctx, byte[] data) {
		switch (resultStatus) {
		case RESULT_STATUS_INIT:
			switch (data[4]) {
			case OkPacket.FIELD_COUNT:
				handleOkPacket(data);
				break;
			case ErrorPacket.FIELD_COUNT:
				handleErrorPacket(data);
				break;
			default:
				resultStatus = RESULT_STATUS_HEADER;
				header = data;
				fields = new ArrayList<byte[]>((int) ByteUtil.readLength(data,
						4));
			}
			break;
		case RESULT_STATUS_HEADER:
			switch (data[4]) {
			case ErrorPacket.FIELD_COUNT:
				resultStatus = RESULT_STATUS_INIT;
				handleErrorPacket(data);
				break;
			case EOFPacket.FIELD_COUNT:
				resultStatus = RESULT_STATUS_FIELD_EOF;
				handleFieldEofPacket(data);
				break;
			default:
				fields.add(data);
			}
			break;
		case RESULT_STATUS_FIELD_EOF:
			switch (data[4]) {
			case ErrorPacket.FIELD_COUNT:
				resultStatus = RESULT_STATUS_INIT;
				handleErrorPacket(data);
				break;
			case EOFPacket.FIELD_COUNT:
				resultStatus = RESULT_STATUS_INIT;
				handleRowEofPacket(data);
				break;
			default:
				handleRowPacket(data);
			}
			break;
		default:
			throw new RuntimeException("unknown status!");
		}
	}

	/**
	 * OK数据包处理
	 */
	private void handleOkPacket(byte[] data) {
		ResponseHandler respHand = con.getRespHandler();
		if (respHand != null) {
			respHand.okResponse(data, con);
		}
	}

	/**
	 * ERROR数据包处理
	 */
	private void handleErrorPacket(byte[] data) {
		ResponseHandler respHand = con.getRespHandler();
		if (respHand != null) {
			respHand.errorResponse(data, con);
		} else {
			closeNoHandler();
		}
	}

	/**
	 * 字段数据包结束处理
	 */
	private void handleFieldEofPacket(byte[] data) {
		ResponseHandler respHand = con.getRespHandler();
		if (respHand != null) {
			respHand.fieldEofResponse(header, fields, data, con);
		} else {
			closeNoHandler();
		}
	}

	/**
	 * 行数据包处理
	 */
	private void handleRowPacket(byte[] data) {
		ResponseHandler respHand = con.getRespHandler();
		if (respHand != null) {
			respHand.rowResponse(data, con);
		} else {
			closeNoHandler();

		}
	}

	private void closeNoHandler() {
		if (!con.isClosed()) {
			con.close("no handler");
			logger.warn("no handler bind in this con " + this + " mysql con:"
					+ con);
		}
	}

	/**
	 * 行数据包结束处理
	 */
	private void handleRowEofPacket(byte[] data) {
		ResponseHandler responseHandler = con.getRespHandler();
		if (responseHandler != null) {
			responseHandler.rowEofResponse(data, con);
		} else {
			closeNoHandler();
		}
	}

}