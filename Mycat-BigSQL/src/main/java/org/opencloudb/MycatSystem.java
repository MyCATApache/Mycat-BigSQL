package org.opencloudb;

import io.netty.bootstrap.Bootstrap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.buffer.BufferPool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.interceptor.SQLInterceptor;
import org.opencloudb.net.FrontSession;
import org.opencloudb.route.MyCATSequnceProcessor;
import org.opencloudb.route.RouteService;
import org.opencloudb.util.ExecutorUtil;
import org.opencloudb.util.NameableExecutor;
import org.opencloudb.util.TimeUtil;

public class MycatSystem {
	public static final String NAME = "MyCat";
	private static final long LOG_WATCH_DELAY = 60000L;
	private static final long TIME_UPDATE_PERIOD = 20L;
	private static final MycatSystem INSTANCE = new MycatSystem();
	private static final Logger LOGGER = Logger.getLogger("MycatServer");
	private final RouteService routerService;
	private final CacheService cacheService;
	private Properties dnIndexProperties;
	private volatile int nextProcessor;
	private final AtomicBoolean isOnline = new AtomicBoolean(true);
	private BufferPool bufferPool;
	private boolean aio = false;
	private final MycatPrivileges privileges;
	private final ConcurrentMap<Long, FrontSession> frontSessions = new ConcurrentHashMap<Long, FrontSession>();
	private final ConcurrentMap<Long, BackendConnection> backends = new ConcurrentHashMap<Long, BackendConnection>();;
	private final MyCATSequnceProcessor sequnceProcessor = new MyCATSequnceProcessor();

	public MyCATSequnceProcessor getSequnceProcessor() {
		return sequnceProcessor;
	}

	public static final MycatSystem getInstance() {
		return INSTANCE;
	}

	private final MycatConfig config;
	private final Timer timer;
	private final long startupTime;
	private final Bootstrap soketConnetor = new Bootstrap();

	private NameableExecutor businessExecutor = ExecutorUtil.create(
			"BusinessExecutor", 10);
	private final SQLInterceptor sqlInterceptor;

	public MycatSystem() {
		this.config = new MycatConfig();
		this.timer = new Timer(NAME + "Timer", true);
		cacheService = new CacheService();
		routerService = new RouteService(cacheService);
		this.startupTime = TimeUtil.currentTimeMillis();
		privileges = new MycatPrivileges();
		try {
			sqlInterceptor = (SQLInterceptor) Class.forName(
					config.getSystem().getSqlInterceptor()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		dnIndexProperties = loadDnIndexProps();
	}

	private Properties loadDnIndexProps() {
		Properties prop = new Properties();
		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + "dnindex.properties");
		if (!file.exists()) {
			return prop;
		}
		FileInputStream filein = null;
		try {
			filein = new FileInputStream(file);
			prop.load(filein);
		} catch (Exception e) {
			LOGGER.warn("load DataNodeIndex err:" + e);
		} finally {
			if (filein != null) {
				try {
					filein.close();
				} catch (IOException e) {
				}
			}
		}
		return prop;
	}

	public CacheService getCacheService() {
		return cacheService;
	}

	public SQLInterceptor getSqlInterceptor() {
		return sqlInterceptor;
	}

	public RouteService getRouterService() {
		return routerService;
	}

	public boolean isOnline() {
		return isOnline.get();
	}

	public void offline() {
		isOnline.set(false);
	}

	/**
	 * save cur datanode index to properties file
	 * 
	 * @param dataNode
	 * @param curIndex
	 */
	public synchronized void saveDataHostIndex(String dataHost, int curIndex) {

		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + "dnindex.properties");
		FileOutputStream fileOut = null;
		try {
			String oldIndex = dnIndexProperties.getProperty(dataHost);
			String newIndex = String.valueOf(curIndex);
			if (newIndex.equals(oldIndex)) {
				return;
			}
			dnIndexProperties.setProperty(dataHost, newIndex);
			LOGGER.info("save DataHost index  " + dataHost + " cur index "
					+ curIndex);

			File parent = file.getParentFile();
			if (parent != null && !parent.exists()) {
				parent.mkdirs();
			}

			fileOut = new FileOutputStream(file);
			dnIndexProperties.store(fileOut, "update");
		} catch (Exception e) {
			LOGGER.warn("saveDataNodeIndex err:", e);
		} finally {
			if (fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
				}
			}
		}

	}

	public ConcurrentMap<Long, BackendConnection> getBackends() {
		return backends;
	}

	public NameableExecutor getBusinessExecutor() {
		return businessExecutor;
	}

	public MycatConfig getConfig() {
		return config;
	}

	public Bootstrap getSoketConnetor() {
		return soketConnetor;
	}

	public ConcurrentMap<Long, FrontSession> getFrontSessions() {
		return frontSessions;
	}

	public MycatPrivileges getPrivileges() {
		return privileges;
	}

}
