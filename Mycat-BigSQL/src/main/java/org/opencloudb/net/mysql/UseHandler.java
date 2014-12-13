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

import org.opencloudb.net.ConnectionInfo;
import org.opencloudb.net.FrontSession;
import org.opencloudb.util.StringUtil;

/**
 * @author mycat
 */
public final class UseHandler {

	public static void handle(String sql, FrontSession session, int offset) {
        String schema = sql.substring(offset).trim();
        int length = schema.length();
        if (length > 0) {
        	if(schema.endsWith(";")) schema=schema.substring(0,schema.length()-1);
        	schema = StringUtil.replaceChars(schema, "`", null);
        	length=schema.length();
            if (schema.charAt(0) == '\'' && schema.charAt(length - 1) == '\'') {
                schema = schema.substring(1, length - 1);
            }
        }
        ConnectionInfo conInfo=session.getConInfo();
        
        String user = conInfo.getUser();
     // check schema
     		if (FrontendAuthenticator.schemaAllowed(session.getCtx(),schema, user,conInfo.getHost())) {
     			session.getConInfo().setSchema(schema);
                 session.writeOK();
     		}
    }
}