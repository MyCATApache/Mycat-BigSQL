package org.opencloudb.route;

import java.sql.SQLNonTransientException;

import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.net.FrontSession;

/**
 * 路由策略接口
 * @author wang.dw
 *
 */
public interface RouteStrategy {
	public RouteResultset route(SystemConfig sysConfig,
			SchemaConfig schema,int sqlType, String origSQL, String charset, FrontSession session, LayerCachePool cachePool)
			throws SQLNonTransientException;
}
