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

import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;

import java.io.IOException;

import org.opencloudb.MycatSystem;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.ConnectionInfo;

/**
 * @author mycat
 */
public class BackMySQLConnectionFactory  {
	public BackMysqlConnection make(BackMySQLConnectionDataSource pool,
			ResponseHandler handler, String schema) throws IOException {

		DBHostConfig dsc = pool.getConfig();
		ConnectionInfo conInfo=new ConnectionInfo();
		MycatSystem.getInstance().getConfig().getSystem().setConnectionParams(conInfo);
		conInfo.setHost(dsc.getIp());
		conInfo.setPort(dsc.getPort());
		conInfo.setUser(dsc.getUser());
		conInfo.setPassword(dsc.getPassword());
		conInfo.setSchema(schema);
		BackMysqlConnection c = new BackMysqlConnection(conInfo,pool);
		// MycatServer.getInstance().getConfig().setSocketParams(c, false);
		
		c.setResponseHandler(handler);
		// c.setIdleTimeout(pool.getConfig().getIdleTimeout());
		ChannelFuture futrue = MycatSystem.getInstance().getSoketConnetor()
				.connect(dsc.getIp(), dsc.getPort());
		futrue.channel()
				.attr(AttributeKey.valueOf(NettyUtil.BACK_MYSQL_CON_KEY))
				.set(c);
		return c;
	}

}