package org.opencloudb.route;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.net.FrontSession;

public class SessionSQLPair {
	public final FrontSession session;
	
	public final SchemaConfig schema;
	public final String sql;
	public final int type;

	public SessionSQLPair(FrontSession session, SchemaConfig schema,
			String sql,int type) {
		super();
		this.session = session;
		this.schema = schema;
		this.sql = sql;
		this.type=type;
	}

}
