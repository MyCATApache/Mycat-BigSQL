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

import static org.opencloudb.server.parser.ServerParseSet.AUTOCOMMIT_OFF;
import static org.opencloudb.server.parser.ServerParseSet.AUTOCOMMIT_ON;
import static org.opencloudb.server.parser.ServerParseSet.CHARACTER_SET_CLIENT;
import static org.opencloudb.server.parser.ServerParseSet.CHARACTER_SET_CONNECTION;
import static org.opencloudb.server.parser.ServerParseSet.CHARACTER_SET_RESULTS;
import static org.opencloudb.server.parser.ServerParseSet.NAMES;
import static org.opencloudb.server.parser.ServerParseSet.TX_READ_COMMITTED;
import static org.opencloudb.server.parser.ServerParseSet.TX_READ_UNCOMMITTED;
import static org.opencloudb.server.parser.ServerParseSet.TX_REPEATED_READ;
import static org.opencloudb.server.parser.ServerParseSet.TX_SERIALIZABLE;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Isolations;
import org.opencloudb.net.FrontSession;
import org.opencloudb.server.parser.ServerParseSet;

/**
 * SET 语句处理
 * 
 * @author mycat
 */
public final class SetHandler {

    private static final Logger logger = Logger.getLogger(SetHandler.class);
    public static void handle(String stmt, FrontSession session, int offset) {
    	//System.out.println("SetHandler: "+stmt);
        int rs = ServerParseSet.parse(stmt, offset);
        switch (rs & 0xff) {
        case AUTOCOMMIT_ON:
            if (session.isAutocommit()) {
            	session.writeOK();
            } else {
            	session.commit();
            	session.setAutocommit(true);
            }
            break;
        case AUTOCOMMIT_OFF: {
            if (session.isAutocommit()) {
            	session.setAutocommit(false);
            }
            session.writeOK();
            break;
        }
        case TX_READ_UNCOMMITTED: {
        	session.getConInfo().setTxIsolation(Isolations.READ_UNCOMMITTED);
        	session.writeOK();
            break;
        }
        case TX_READ_COMMITTED: {
        	session.getConInfo().setTxIsolation(Isolations.READ_COMMITTED);
        	session.writeOK();
            break;
        }
        case TX_REPEATED_READ: {
        	session.getConInfo().setTxIsolation(Isolations.REPEATED_READ);
        	session.writeOK();
            break;
        }
        case TX_SERIALIZABLE: {
        	session.getConInfo().setTxIsolation(Isolations.SERIALIZABLE);
        	session.writeOK();
            break;
        }
        case NAMES:
            String charset = stmt.substring(rs >>> 8).trim();
            if (session.getConInfo().setCharset(charset)) {
            	session.writeOK();
            } else {
            	session.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
            }
            break;
        case CHARACTER_SET_CLIENT:
        case CHARACTER_SET_CONNECTION:
        case CHARACTER_SET_RESULTS:
            CharacterSet.response(stmt, session, rs);
            break;
        default:
            StringBuilder s = new StringBuilder();
            logger.warn(s.append(session.getConInfo()).append(' ').append(stmt).append(" is not recoginized and ignored").toString());
            session.writeOK();
        }
    }

}