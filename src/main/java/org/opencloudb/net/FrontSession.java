package org.opencloudb.net;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatSystem;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.ConnectionMeta;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.mpp.DataMergeService;
import org.opencloudb.mysql.nio.handler.CommitNodeHandler;
import org.opencloudb.mysql.nio.handler.MultiNodeCoordinator;
import org.opencloudb.mysql.nio.handler.MultiNodeQueryHandler;
import org.opencloudb.mysql.nio.handler.RollbackNodeHandler;
import org.opencloudb.mysql.nio.handler.RollbackReleaseHandler;
import org.opencloudb.mysql.nio.handler.SingleNodeHandler;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.KillConnectionHandler;
import org.opencloudb.net.mysql.NettyUtil;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.sqlcmd.SQLCmdConstant;

public class FrontSession implements Session {
	private static final Logger LOGGER = Logger.getLogger(FrontSession.class);
	private volatile boolean autocommit;
	private final ChannelHandlerContext ctx;
	private final ConnectionInfo conInf;
	private final boolean readOnly;
	private final ConcurrentHashMap<RouteResultsetNode, BackendConnection> target = new ConcurrentHashMap<RouteResultsetNode, BackendConnection>();
	final MultiNodeCoordinator multiNodeCoordinator;
	final CommitNodeHandler commitHandler;
	private RollbackNodeHandler rollbackHandler;
	private SingleNodeHandler singleNodeHandler;
	private MultiNodeQueryHandler multiNodeHandler;
	private final AtomicBoolean isClosed = new AtomicBoolean(false);
	private volatile boolean txInterrupted;
	private volatile String txInterrputMsg = "";
	private long lastInsertId;

	public FrontSession(ChannelHandlerContext frontCtx, boolean isReadOnly,
			ConnectionInfo conInf) {
		super();
		this.ctx = frontCtx;
		this.readOnly = isReadOnly;
		
		this.conInf = conInf;
		multiNodeCoordinator = new MultiNodeCoordinator(this);
		commitHandler = new CommitNodeHandler(this);
		
		
	}

	@Override
	public int getTargetCount() {
		return target.size();
	}

	public Set<RouteResultsetNode> getTargetKeys() {
		return target.keySet();
	}

	public BackendConnection getTarget(RouteResultsetNode key) {
		return target.get(key);
	}

	public Map<RouteResultsetNode, BackendConnection> getTargetMap() {
		return this.target;
	}

	public BackendConnection removeTarget(RouteResultsetNode key) {
		return target.remove(key);
	}

	/**
	 * 提交事务
	 */
	public void commit() {
		if (txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES,
					"Transaction error, need to rollback.");
		} else {
			final int initCount = target.size();
			if (initCount <= 0) {
				this.writeOK();
				return;
			} else if (initCount == 1) {
				BackendConnection con = target.elements().nextElement();
				commitHandler.commit(con);

			} else {

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("multi node commit to send ,total "
							+ initCount);
				}
				multiNodeCoordinator
						.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);
			}
		}
	}

	/**
	 * 回滚事务
	 */
	public void rollback() {
		// 状态检查
		if (txInterrupted) {
			txInterrupted = false;
		}
		// 执行回滚
		final int initCount = target.size();
		if (initCount <= 0) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("no session bound connections found ,no need send rollback cmd ");
			}
			this.writeOK();
			return;
		}
		rollbackHandler = new RollbackNodeHandler(this);
		rollbackHandler.rollback();
	}

	public long getLastInsertId() {
		return lastInsertId;
	}

	public void setLastInsertId(long lastInsertId) {
		this.lastInsertId = lastInsertId;
	}

	/**
	 * 设置是否需要中断当前事务
	 */
	public void setTxInterrupt(String txInterrputMsg) {
		if (!autocommit && !txInterrupted) {
			txInterrupted = true;
			this.txInterrputMsg = txInterrputMsg;
		}
	}

	public boolean isTxInterrupted() {
		return txInterrupted;
	}

	@Override
	public void execute(RouteResultset rrs, int type) {
		// clear prev execute resources
		clearHandlesResources();
		if (LOGGER.isDebugEnabled()) {
			StringBuilder s = new StringBuilder();
			LOGGER.debug(s.append(this.conInf.toString()).append(rrs)
					.toString()
					+ " rrs ");
		}

		// 检查路由结果是否为空
		RouteResultsetNode[] nodes = rrs.getNodes();
		if (nodes == null || nodes.length == 0 || nodes[0].getName() == null
				|| nodes[0].getName().equals("")) {
			writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
					"No dataNode found ,please check tables defined in schema:"
							+ this.conInf.getSchema());
			return;
		}

		if (nodes.length == 1) {
			singleNodeHandler = new SingleNodeHandler(rrs, this);
			try {
				singleNodeHandler.execute();
			} catch (Exception e) {
				LOGGER.warn(new StringBuilder().append(this.conInf.toString())
						.append(rrs), e);
				writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
			}
		} else {
			boolean autocommit = this.autocommit;
			DataMergeService dataMergeSvr = null;
			if (ServerParse.SELECT == type && rrs.needMerge()) {
				dataMergeSvr = new DataMergeService(rrs);
			}
			multiNodeHandler = new MultiNodeQueryHandler(rrs, autocommit, this,
					dataMergeSvr);

			try {
				multiNodeHandler.execute();
			} catch (Exception e) {
				LOGGER.warn(new StringBuilder().append(this.conInf.toString())
						.append(rrs), e);
				writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
			}
		}
	}

	/**
	 * {@link ServerConnection#isClosed()} must be true before invoking this
	 */
	public void terminate() {
		for (BackendConnection node : target.values()) {
			node.close("client closed ");
		}
		clearHandlesResources();
	}

	public void releaseConnectionIfSafe(BackendConnection conn, boolean debug,
			boolean needRollback) {
		RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();

		if (node != null) {
			if (this.autocommit || conn.isFromSlaveDB()
					|| !conn.isModifiedSQLExecuted()) {
				releaseConnection((RouteResultsetNode) conn.getAttachment(),
						LOGGER.isDebugEnabled(), needRollback);
			}
		}
	}

	public void releaseConnection(RouteResultsetNode rrn, boolean debug,
			final boolean needRollback) {

		BackendConnection c = target.remove(rrn);
		if (c != null) {
			if (debug) {
				LOGGER.debug("release connection " + c);
			}
			if (c.getAttachment() != null) {
				c.setAttachment(null);
			}
			if (!c.isClosed()) {
				if (c.isAutocommit()) {
					c.release();
				} else if (needRollback) {
					c.setResponseHandler(new RollbackReleaseHandler());
					c.rollback();
				} else {
					c.release();
				}
			}
		}
	}

	public void releaseConnections(final boolean needRollback) {
		boolean debug = LOGGER.isDebugEnabled();
		for (RouteResultsetNode rrn : target.keySet()) {
			releaseConnection(rrn, debug, needRollback);
		}
	}

	public void releaseConnection(BackendConnection con) {
		Iterator<Entry<RouteResultsetNode, BackendConnection>> itor = target
				.entrySet().iterator();
		while (itor.hasNext()) {
			BackendConnection theCon = itor.next().getValue();
			if (theCon == con) {
				itor.remove();
				con.release();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("realse connection " + con);
				}
				break;
			}
		}

	}

	/**
	 * @return previous bound connection
	 */
	public BackendConnection bindConnection(RouteResultsetNode key,
			BackendConnection conn) {
		// System.out.println("bind connection "+conn+
		// " to key "+key.getName()+" on sesion "+this);
		return target.put(key, conn);
	}

	public boolean tryExistsCon(final BackendConnection conn,
			RouteResultsetNode node) {

		if (conn == null) {
			return false;
		}
		if (!conn.isFromSlaveDB() || node.canRunnINReadDB(this.autocommit)) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("found connections in session to use " + conn
						+ " for " + node);
			}
			conn.setAttachment(node);
			return true;
		} else {
			// slavedb connection and can't use anymore ,release it
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("release slave connection,can't be used in trasaction  "
						+ conn + " for " + node);
			}
			releaseConnection(node, LOGGER.isDebugEnabled(), false);
		}
		return false;
	}

	protected void kill() {
		boolean hooked = false;
		AtomicInteger count = null;
		Map<RouteResultsetNode, BackendConnection> killees = null;
		for (RouteResultsetNode node : target.keySet()) {
			BackendConnection c = target.get(node);
			if (c != null) {
				if (!hooked) {
					hooked = true;
					killees = new HashMap<RouteResultsetNode, BackendConnection>();
					count = new AtomicInteger(0);
				}
				killees.put(node, c);
				count.incrementAndGet();
			}
		}
		if (hooked) {
			ConnectionMeta conMeta = new ConnectionMeta(null, null, -1, true);
			for (Entry<RouteResultsetNode, BackendConnection> en : killees
					.entrySet()) {
				KillConnectionHandler kill = new KillConnectionHandler(
						en.getValue(), this);
				MycatConfig conf = MycatSystem.getInstance().getConfig();
				PhysicalDBNode dn = conf.getDataNodes().get(
						en.getKey().getName());
				try {
					dn.getConnectionFromSameSource(conMeta, en.getValue(),
							kill, en.getKey());
				} catch (Exception e) {
					LOGGER.error(
							"get killer connection failed for " + en.getKey(),
							e);
					kill.connectionError(e, null);
				}
			}
		}
	}

	private void clearHandlesResources() {
		SingleNodeHandler singleHander = singleNodeHandler;
		if (singleHander != null) {
			singleHander.clearResources();
			singleNodeHandler = null;
		}
		MultiNodeQueryHandler multiHandler = multiNodeHandler;
		if (multiHandler != null) {
			multiHandler.clearResources();
			multiNodeHandler = null;
		}
	}

	public void clearResources(final boolean needRollback) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("clear session resources " + this);
		}
		this.releaseConnections(needRollback);
		clearHandlesResources();
	}

	public boolean closed() {
		return isClosed.get();
	}

	/**
	 * allowcate small
	 * 
	 * @return
	 */
	public ByteBuf allocate(int initSize, int maxSize) {
		return ctx.alloc().ioBuffer(initSize, maxSize);
	}

	public ByteBuf allocate(int size) {
		return ctx.alloc().ioBuffer(size);
	}

	public boolean isAutocommit() {
		return autocommit;
	}

	public void setAutocommit(boolean autocommit) {
		this.autocommit = autocommit;
	}

	/**
	 * content.addComponent(chunk.content()); need folowing code
	 * content.writerIndex(content.writerIndex() +
	 * chunk.content().readableBytes());
	 * 
	 * @return
	 */
	public CompositeByteBuf allocateCompositeByteBuf(int maxCount) {
		return ctx.alloc().compositeBuffer(maxCount);
	}

	public ChannelHandlerContext getCtx() {
		return ctx;
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	@Override
	public void close(String reason) {

		if (isClosed.compareAndSet(false, true)) {
			for (BackendConnection node : target.values()) {
				node.close("client closed ");
			}
			clearHandlesResources();
		}
	}

	public void writeOK() {
		NettyUtil.writeBytes(ctx, OkPacket.OK);

	}

	public void writeBytes(byte[] bytes) {
		NettyUtil.writeBytes(ctx, bytes);
	}

	public void writeErrMessage(int errno, String msg) {
		NettyUtil.writeErrMessage(ctx, errno, msg);

	}

	public void write(ByteBuf buffer) {
		ctx.writeAndFlush(buffer);

	}

	public void writeNoFlush(ByteBuf buffer) {
		ctx.write(buffer);

	}

	public void writeNoFlush(byte[] buffer) {
		NettyUtil.writeBytesNoFlush(ctx, buffer);

	}

	public void writeErrorPkg(ErrorPacket pkg) {
		ByteBuf buf = ctx.alloc().ioBuffer(pkg.calcPacketSize() + 4);
		pkg.write(buf);
		ctx.writeAndFlush(buf);

	}

	public void execute(String sql, int type) {
		if (this.isClosed.get()) {
			LOGGER.warn("ignore execute ,server connection is closed " + this);
			return;
		}
		// 状态检查
		if (txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES,
					"Transaction error, need to rollback." + txInterrputMsg);
			return;
		}

		// 检查当前使用的DB
		String db = this.getConInfo().getSchema();
		if (db == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"No MyCAT Database selected");
			return;
		}
		SchemaConfig schema = MycatSystem.getInstance().getConfig()
				.getSchemas().get(db);
		if (schema == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"Unknown MyCAT Database '" + db + "'");
			return;
		}

		routeEndExecuteSQL(sql, type, schema);

	}

	public void routeEndExecuteSQL(String sql, int type, SchemaConfig schema) {
		// 路由计算
		RouteResultset rrs = null;
		try {
			rrs = MycatSystem
					.getInstance()
					.getRouterService()
					.route(MycatSystem.getInstance().getConfig().getSystem(),
							schema, type, sql, this.getConInfo().getCharset(),
							this);

		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			LOGGER.warn(s.append(this).append(sql).toString() + " err:"
					+ e.toString());
			String msg = e.getMessage();
			writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e
					.getClass().getSimpleName() : msg);
			return;
		}
		if (rrs != null) {
			// session执行
			execute(rrs, type);
		}
	}

	@Override
	public ConnectionInfo getConInfo() {
		return this.conInf;
	}

	public void writeOK(OkPacket ok) {
		writeOK();
//		ByteBuf buf=ctx.alloc().ioBuffer(ok.calcPacketSize() + 4);
//		ok.write(buffer, c, writeSocketIfFull)
//		ctx.writeAndFlush(OkPacket.OK);

	}

}
