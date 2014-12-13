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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.opencloudb.MycatCluster;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatNode;
import org.opencloudb.MycatSystem;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.MycatNodeConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.FrontSession;
import org.opencloudb.util.IntegerUtil;
import org.opencloudb.util.StringUtil;

/**
 * @author mycat
 */
public class ShowMyCATCluster {

private static final int FIELD_COUNT = 2;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("WEIGHT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
	}

	public static void response(FrontSession session) {
		ByteBuf buffer = session.allocate(512, 1024);

		// write header
		buffer = header.write(buffer);

		// write field
		for (FieldPacket field : fields) {
			buffer = field.write(buffer);
		}

		// write eof
		buffer = eof.write(buffer);

		// write rows
		byte packetId = eof.packetId;
		for (RowDataPacket row : getRows(session)) {
			row.packetId = ++packetId;
			buffer = row.write(buffer);
		}

		// last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer);

		// post write
		session.write(buffer);
	}

	private static List<RowDataPacket> getRows(FrontSession session) {
		List<RowDataPacket> rows = new LinkedList<RowDataPacket>();
		MycatConfig config = MycatSystem.getInstance().getConfig();
		MycatCluster cluster = config.getCluster();
		Map<String, SchemaConfig> schemas = config.getSchemas();
		String theSchema = session.getConInfo().getSchema();
		SchemaConfig schema = (theSchema == null) ? null : schemas
				.get(theSchema);
		String charSet = session.getConInfo().getCharset();
		// 如果没有指定schema或者schema为null，则使用全部集群。
		if (schema == null) {
			Map<String, MycatNode> nodes = cluster.getNodes();
			for (MycatNode n : nodes.values()) {
				if (n != null && n.isOnline()) {
					rows.add(getRow(n, charSet));
				}
			}
		} else {

			Map<String, MycatNode> nodes = cluster.getNodes();
			for (MycatNode n : nodes.values()) {
				if (n != null && n.isOnline()) {
					rows.add(getRow(n, charSet));
				}
			}
		}

		return rows;
	}

	private static RowDataPacket getRow(MycatNode node, String charset) {
		MycatNodeConfig conf = node.getConfig();
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(conf.getHost(), charset));
		row.add(IntegerUtil.toBytes(conf.getWeight()));
		return row;
	}

}