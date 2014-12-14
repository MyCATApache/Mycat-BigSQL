package org.opencloudb.net;

import org.opencloudb.route.RouteResultset;

public interface Session {

	public void close(String reason);

	/**
	 * 取得源端连接
	 */
	ConnectionInfo getConInfo();

	/**
	 * 取得当前目标端数量
	 */
	int getTargetCount();

	/**
	 * 开启一个会话执行
	 */
	void execute(RouteResultset rrs, int type);

	/**
	 * 提交一个会话执行
	 */
	void commit();

	/**
	 * 回滚一个会话执行
	 */
	void rollback();

	/**
	 * 终止会话，必须在关闭源端连接后执行该方法。
	 */
	void terminate();

}
