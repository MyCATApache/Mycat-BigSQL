package org.opencloudb.backend;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.ClosableConnection;
import org.opencloudb.net.ConnectionInfo;
import org.opencloudb.net.FrontSession;
import org.opencloudb.route.RouteResultsetNode;

public interface BackendConnection extends ClosableConnection {
	
	public boolean isModifiedSQLExecuted();

	public boolean isFromSlaveDB();

	public String getSchema();

	public void setSchema(String newSchema);

	public long getLastTime();

	public void setAttachment(Object attachment);

	public void quit();

	public void setLastTime(long currentTimeMillis);

	public void release();

	public void commit();

	public void query(String sql) throws UnsupportedEncodingException;

	public Object getAttachment();

	public void execute(RouteResultsetNode node, FrontSession session,
			boolean autocommit) throws IOException;

	public void recordSql(String host, String schema, String statement);

	public boolean syncAndExcute();

	public void rollback();

	public boolean isBorrowed();

	public void setBorrowed(boolean borrowed);

	public boolean isAutocommit();

	public long getId();

	public void setResponseHandler(ResponseHandler responseHandler);

}
