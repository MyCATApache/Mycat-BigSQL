package org.opencloudb.net.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.config.Isolations;
import org.opencloudb.exception.UnknownTxIsolationException;
import org.opencloudb.mysql.CharsetUtil;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.ConnectionInfo;
import org.opencloudb.net.FrontSession;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.TimeUtil;

public class BackMysqlConnection implements BackendConnection {
	private static final Logger LOGGER = Logger
			.getLogger(BackMysqlConnection.class);
	private ResponseHandler respHandler;
	private final BackMySQLConnectionDataSource pool;
	private volatile StatusSync statusSync;
	private volatile boolean oldAutoCommit;
	private final AtomicBoolean isClosed = new AtomicBoolean();
	private final boolean fromeSlave;
	private HandshakePacket handshake;
	private final ConnectionInfo conInfo;
	private volatile boolean autocommit;
	private boolean authenticated;
	private volatile long lastTime; // QS_TODO
	private volatile String oldSchema;
	private volatile boolean borrowed = false;
	private volatile boolean modifiedSQLExecuted = false;
	private volatile boolean txSetCmdExecuted = false;
	private static final CommandPacket _READ_UNCOMMITTED = new CommandPacket();
	private static final CommandPacket _READ_COMMITTED = new CommandPacket();
	private static final CommandPacket _REPEATED_READ = new CommandPacket();
	private static final CommandPacket _SERIALIZABLE = new CommandPacket();
	private static final CommandPacket _AUTOCOMMIT_ON = new CommandPacket();
	private static final CommandPacket _AUTOCOMMIT_OFF = new CommandPacket();
	private static final CommandPacket _COMMIT = new CommandPacket();
	private static final CommandPacket _ROLLBACK = new CommandPacket();
	private ChannelHandlerContext ctx;
	private Object attachment;
	static {
		_READ_UNCOMMITTED.packetId = 0;
		_READ_UNCOMMITTED.command = MySQLPacket.COM_QUERY;
		_READ_UNCOMMITTED.arg = "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
				.getBytes();
		_READ_COMMITTED.packetId = 0;
		_READ_COMMITTED.command = MySQLPacket.COM_QUERY;
		_READ_COMMITTED.arg = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"
				.getBytes();
		_REPEATED_READ.packetId = 0;
		_REPEATED_READ.command = MySQLPacket.COM_QUERY;
		_REPEATED_READ.arg = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
				.getBytes();
		_SERIALIZABLE.packetId = 0;
		_SERIALIZABLE.command = MySQLPacket.COM_QUERY;
		_SERIALIZABLE.arg = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"
				.getBytes();
		_AUTOCOMMIT_ON.packetId = 0;
		_AUTOCOMMIT_ON.command = MySQLPacket.COM_QUERY;
		_AUTOCOMMIT_ON.arg = "SET autocommit=1".getBytes();
		_AUTOCOMMIT_OFF.packetId = 0;
		_AUTOCOMMIT_OFF.command = MySQLPacket.COM_QUERY;
		_AUTOCOMMIT_OFF.arg = "SET autocommit=0".getBytes();
		_COMMIT.packetId = 0;
		_COMMIT.command = MySQLPacket.COM_QUERY;
		_COMMIT.arg = "commit".getBytes();
		_ROLLBACK.packetId = 0;
		_ROLLBACK.command = MySQLPacket.COM_QUERY;
		_ROLLBACK.arg = "rollback".getBytes();
	}

	public ChannelHandlerContext getCtx() {
		return ctx;
	}

	public void setCtx(ChannelHandlerContext ctx) {
		this.ctx = ctx;
	}

	public long getThreadId() {
		return conInfo.getThreadId();
	}

	public ConnectionInfo getConInfo() {
		return conInfo;
	}

	public void setAutocommit(boolean autocommit) {
		this.autocommit = autocommit;
	}

	public void setThreadId(long threadId) {
		this.conInfo.setThreadId(threadId);
	}

	public HandshakePacket getHandshake() {
		return handshake;
	}

	public void setHandshake(HandshakePacket handshake) {
		this.handshake = handshake;
	}

	public boolean isFromeSlave() {
		return fromeSlave;
	}

	public boolean isBorrowed() {
		return borrowed;
	}

	public void setBorrowed(boolean borrowed) {
		this.borrowed = borrowed;
	}

	public BackMysqlConnection(ConnectionInfo conInfo,
			BackMySQLConnectionDataSource pool) {
		super();
		this.conInfo = conInfo;
		this.fromeSlave = pool.isReadNode();
		this.pool = pool;
	}

	public String getPassword() {
		return conInfo.getPassword();
	}

	public void setPassword(String password) {
		this.conInfo.setPassword(password);
	}

	public String getUser() {
		return conInfo.getUser();
	}

	public void setUser(String user) {
		this.conInfo.setUser(user);
	}

	public void setHost(String host) {
		this.conInfo.setHost(host);
	}

	public ResponseHandler getRespHandler() {
		return respHandler;
	}

	protected void sendQueryCmd(String query) {
		CommandPacket packet = new CommandPacket();
		packet.packetId = 0;
		packet.command = MySQLPacket.COM_QUERY;
		try {
			packet.arg = query.getBytes(this.conInfo.getCharset());
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		lastTime = TimeUtil.currentTimeMillis();
		this.writeCmdPkg(packet);
	}

	public void setResponseHandler(ResponseHandler respHandler) {
		this.respHandler = respHandler;
	}

	public boolean isAuthenticated() {
		return authenticated;
	}

	public void setPort(int port) {
		this.conInfo.setPort(port);
	}

	@Override
	public String getCharset() {
		return conInfo.getCharset();
	}

	public void close(String reason) {
		if (!isClosed.get()) {
			ctx.close();
			pool.connectionClosed(this);
			if (this.respHandler != null) {
				this.respHandler.connectionClose(this, reason);
				respHandler = null;
			}
		}
	}

	@Override
	public boolean isClosed() {
		return this.isClosed.get();
	}

	@Override
	public void idleCheck() {
		// TODO Auto-generated method stub

	}

	@Override
	public long getStartupTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getHost() {
		return conInfo.getHost();
	}

	@Override
	public int getPort() {
		return conInfo.getPort();
	}

	@Override
	public int getLocalPort() {

		return conInfo.getLocalPort();
	}

	@Override
	public long getNetInBytes() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNetOutBytes() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isModifiedSQLExecuted() {
		return this.modifiedSQLExecuted;
	}

	@Override
	public boolean isFromSlaveDB() {
		return this.fromeSlave;
	}

	@Override
	public String getSchema() {
		return conInfo.getSchema();
	}

	@Override
	public void setSchema(String newSchema) {
		conInfo.setSchema(newSchema);

	}

	@Override
	public long getLastTime() {
		return this.lastTime;
	}

	@Override
	public void setAttachment(Object attachment) {
		this.attachment = attachment;

	}

	@Override
	public void setLastTime(long currentTimeMillis) {
		lastTime = currentTimeMillis;

	}

	@Override
	public Object getAttachment() {
		return attachment;
	}

	@Override
	public void recordSql(String host, String schema, String statement) {
		// TODO Auto-generated method stub

	}

	public void writeCmdPkg(CommandPacket cmdPkg) {
		ByteBuf buf = ctx.alloc().ioBuffer(cmdPkg.calcPacketSize() + 4);
		cmdPkg.write(buf);
		ctx.writeAndFlush(buf);
		// System.out.println("write query command "+cmdPkg);
	}

	@Override
	public boolean isAutocommit() {
		return autocommit;
	}

	@Override
	public long getId() {
		return conInfo.getId();
	}

	public void query(String query) throws UnsupportedEncodingException {
		RouteResultsetNode rrn = new RouteResultsetNode("default",
				ServerParse.SELECT, query);
		StatusSync sync = new StatusSync(this, rrn,
				this.conInfo.getCharsetIndex(), this.conInfo.getTxIsolation(),
				true);
		doExecute(sync);
	}

	private void doExecute(StatusSync sync) {
		statusSync = sync;
		if (sync.isSync() || !sync.sync()) {
			sync.execute();
		}
	}

	public void quit() {
		if (!this.isClosed()) {
			if (this.authenticated) {
				NettyUtil.writeBytes(ctx, QuitPacket.QUIT);
				// QS_TODO check
				ctx.writeAndFlush(ctx.alloc().ioBuffer(1));
			} else {
				close("normal");
			}
		}
	}

	/**
	 * @return if synchronization finished and execute-sql has already been sent
	 *         before
	 */
	public boolean syncAndExcute() {
		StatusSync sync = statusSync;
		if (sync.isExecuted()) {
			return true;
		}
		if (sync.isSync()) {
			sync.update();
			sync.execute();
		} else {
			sync.update();
			sync.sync();
		}
		return false;
	}

	public void execute(RouteResultsetNode rrn, FrontSession session,
			boolean autocommit) throws UnsupportedEncodingException {
		if (!modifiedSQLExecuted && rrn.isModifySQL()) {
			modifiedSQLExecuted = true;
		}
		StatusSync sync = new StatusSync(this, rrn,
				this.conInfo.getCharsetIndex(), session.getConInfo()
						.getTxIsolation(), autocommit);
		doExecute(sync);
	}

	public void commit() {
		ByteBuf buf = ctx.alloc().ioBuffer(128);
		_COMMIT.write(buf);
		ctx.writeAndFlush(buf);
		txSetCmdExecuted = false;
	}

	public void rollback() {
		ByteBuf buf = ctx.alloc().ioBuffer(128);
		_ROLLBACK.write(buf);
		ctx.writeAndFlush(buf);
		txSetCmdExecuted = false;
	}

	public void release() {
		attachment = null;
		statusSync = null;
		modifiedSQLExecuted = false;
		setResponseHandler(null);
		pool.releaseChannel(this);
		txSetCmdExecuted = false;
	}

	public void setAuthenticated(boolean b) {
		authenticated = b;

	}

	public BackMySQLConnectionDataSource getPool() {
		return pool;
	}

	private static class StatusSync {
		private final RouteResultsetNode rrn;
		private final BackMysqlConnection conn;
		private CommandPacket schemaCmd;
		private CommandPacket charCmd;
		private CommandPacket isoCmd;
		private CommandPacket acCmd;
		private final String schema;
		private final int charIndex;
		private final int txIsolation;
		private final boolean autocommit;
		private volatile boolean executed;

		public StatusSync(BackMysqlConnection conn, RouteResultsetNode rrn,
				int scCharIndex, int scTxtIsolation, boolean autocommit) {
			this.conn = conn;
			this.rrn = rrn;
			this.charIndex = scCharIndex;
			this.schema = conn.getConInfo().getSchema();
			this.schemaCmd = !schema.equals(conn.oldSchema) ? getChangeSchemaCommand(schema)
					: null;
			this.charCmd = conn.getConInfo().getCharsetIndex() != charIndex ? getCharsetCommand(charIndex)
					: null;
			this.txIsolation = scTxtIsolation;

			this.isoCmd = conn.getConInfo().getTxIsolation() != txIsolation ? getTxIsolationCommand(txIsolation)
					: null;
			if (!conn.modifiedSQLExecuted || conn.isFromSlaveDB()) {
				// never executed modify sql,so auto commit
				this.autocommit = true;
			} else {
				this.autocommit = autocommit;
			}
			if (this.autocommit) {
				this.acCmd = (conn.autocommit == true) ? null : _AUTOCOMMIT_ON;
			} else {// transaction
				if (!conn.txSetCmdExecuted) {
					this.acCmd = _AUTOCOMMIT_OFF;
				}
			}

			if (LOGGER.isDebugEnabled()) {
				StringBuilder inf = new StringBuilder();
				if (schemaCmd != null) {
					inf.append("   need syn schemaCmd " + schemaCmd + "\r\n");
				}
				if (charCmd != null) {
					inf.append("   need syn charCmd " + charCmd + "\r\n");
				}
				if (isoCmd != null) {
					inf.append("   need syn txIsolationCmd " + isoCmd + "\r\n");
				}
				if (acCmd != null) {
					inf.append("   need syn autcommitCmd " + acCmd + "\r\n");
				}
				if (inf.length() > 0) {
					LOGGER.debug(this.conn + "\r\n" + inf);
				}
			}

		}

		private Runnable updater;

		public boolean isExecuted() {
			return executed;
		}

		public boolean isSync() {
			return schemaCmd == null && charCmd == null && isoCmd == null
					&& acCmd == null;
		}

		public void update() {
			Runnable updater = this.updater;
			if (updater != null) {
				updater.run();
			}
		}

		/**
		 * @return false if sync complete
		 */
		public boolean sync() {
			CommandPacket cmd;
			if (schemaCmd != null) {
				conn.conInfo.setSchema("snyn...");
				updater = new Runnable() {
					@Override
					public void run() {
						conn.conInfo.setSchema(schema);
						conn.oldSchema = schema;
					}
				};
				cmd = schemaCmd;
				schemaCmd = null;
				conn.writeCmdPkg(cmd);
				// System.out.println("syn schema "+conn+" schema "+schema);
				return true;
			}
			if (charCmd != null) {
				updater = new Runnable() {
					@Override
					public void run() {
						int ci = StatusSync.this.charIndex;
						conn.getConInfo().setCharsetIndex(ci);
					}
				};
				cmd = charCmd;
				charCmd = null;
				conn.writeCmdPkg(cmd);
				// System.out.println("syn charCmd "+conn);
				return true;
			}
			if (isoCmd != null) {
				updater = new Runnable() {
					@Override
					public void run() {
						conn.getConInfo().setTxIsolation(
								StatusSync.this.txIsolation);
					}
				};
				cmd = isoCmd;
				isoCmd = null;
				conn.writeCmdPkg(cmd);
				// System.out.println("syn iso "+conn);
				return true;
			}
			if (acCmd != null) {
				conn.autocommit = conn.oldAutoCommit;
				updater = new Runnable() {
					@Override
					public void run() {
						conn.autocommit = StatusSync.this.autocommit;
						conn.oldAutoCommit = autocommit;
						if (StatusSync.this.autocommit == false) {
							conn.txSetCmdExecuted = true;
						}
					}
				};
				cmd = acCmd;
				acCmd = null;
				conn.writeCmdPkg(cmd);
				// System.out.println("syn autocomit "+conn);
				return true;
			}
			return false;
		}

		public void execute() {
			executed = true;
			if (rrn.getStatement() != null) {
				conn.sendQueryCmd(rrn.getStatement());
			}

		}

		@Override
		public String toString() {
			return "StatusSync [schemaCmd=" + schemaCmd + ", charCmd="
					+ charCmd + ", isoCmd=" + isoCmd + ", acCmd=" + acCmd
					+ ", executed=" + executed + "]";
		}

		private static CommandPacket getTxIsolationCommand(int txIsolation) {
			switch (txIsolation) {
			case Isolations.READ_UNCOMMITTED:
				return _READ_UNCOMMITTED;
			case Isolations.READ_COMMITTED:
				return _READ_COMMITTED;
			case Isolations.REPEATED_READ:
				return _REPEATED_READ;
			case Isolations.SERIALIZABLE:
				return _SERIALIZABLE;
			default:
				throw new UnknownTxIsolationException("txIsolation:"
						+ txIsolation);
			}
		}

		private static CommandPacket getCharsetCommand(int ci) {
			String charset = CharsetUtil.getCharset(ci);
			StringBuilder s = new StringBuilder();
			s.append("SET names ").append(charset);
			CommandPacket cmd = new CommandPacket();
			cmd.packetId = 0;
			cmd.command = MySQLPacket.COM_QUERY;
			cmd.arg = s.toString().getBytes();
			return cmd;
		}

		private static CommandPacket getChangeSchemaCommand(String schema) {
			StringBuilder s = new StringBuilder();
			s.append(schema);
			CommandPacket cmd = new CommandPacket();
			cmd.packetId = 0;
			cmd.command = MySQLPacket.COM_INIT_DB;
			cmd.arg = s.toString().getBytes();
			return cmd;
		}
	}

	@Override
	public String toString() {
		return "BackMysqlConnection [ conInfo=" + conInfo+",respHandler=" + respHandler
				+ ", fromeSlave=" + fromeSlave 
				+ ", autocommit=" + autocommit + ", authenticated="
				+ authenticated + ", borrowed=" + borrowed
				+ ", modifiedSQLExecuted=" + modifiedSQLExecuted + "]";
	}
}
