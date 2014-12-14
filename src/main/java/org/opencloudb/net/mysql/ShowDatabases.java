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

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatSystem;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.FrontSession;
import org.opencloudb.util.StringUtil;

/**
 * @author mycat
 */
public class ShowDatabases {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("DATABASE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }

    public static void response(FrontSession session) {
    	ByteBuf buffer = session.allocate(512, 1024*4);

        // write header
        buffer = header.write(buffer);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer);
        }

        // write eof
        buffer = eof.write(buffer);

        // write rows
        byte packetId = eof.packetId;
        MycatConfig conf = MycatSystem.getInstance().getConfig();
        Map<String, UserConfig> users = conf.getUsers();
        UserConfig user = users == null ? null : users.get(session.getConInfo().getUser());
        if (user != null) {
            TreeSet<String> schemaSet = new TreeSet<String>();
            Set<String> schemaList = user.getSchemas();
            if (schemaList == null || schemaList.size() == 0) {
                schemaSet.addAll(conf.getSchemas().keySet());
            } else {
                for (String schema : schemaList) {
                    schemaSet.add(schema);
                }
            }
            for (String name : schemaSet) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(name, session.getConInfo().getCharset()));
                row.packetId = ++packetId;
                buffer = row.write(buffer);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer);

        // post write
        session.write(buffer);
    }

}