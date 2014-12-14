package org.opencloudb.route.handler;

import java.sql.SQLNonTransientException;

import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.net.FrontSession;
import org.opencloudb.route.RouteResultset;

/**
 * 按照注释中包含指定类型的内容做路由解析
 * 
 */
public interface HintHandler {

	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
			int sqlType, String realSQL, String charset, FrontSession session,
			LayerCachePool cachePool, String hintSQLValue)
			throws SQLNonTransientException;
}
