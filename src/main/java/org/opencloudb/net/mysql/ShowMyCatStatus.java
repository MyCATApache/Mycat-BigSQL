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

import org.opencloudb.MycatSystem;
import org.opencloudb.config.Fields;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.FrontSession;

/**
 * 加入了offline状态推送，用于心跳语句。
 * 
 * @author mycat
 * @author mycat
 */
public class ShowMyCatStatus {

	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	private static final RowDataPacket status = new RowDataPacket(FIELD_COUNT);
	private static final EOFPacket lastEof = new EOFPacket();
	private static final ErrorPacket error = PacketUtil.getShutdown();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
		status.add("ON".getBytes());
		status.packetId = ++packetId;
		lastEof.packetId = ++packetId;
	}

	public static void response(FrontSession session) {
		ByteBuf buffer = session.allocate(512, 1024);
		if (MycatSystem.getInstance().isOnline()) {

			buffer = header.write(buffer);
			for (FieldPacket field : fields) {
				buffer = field.write(buffer);
			}
			buffer = eof.write(buffer);
			buffer = status.write(buffer);
			buffer = lastEof.write(buffer);
		} else {
			error.write(buffer);
		}
		session.write(buffer);
	}

}