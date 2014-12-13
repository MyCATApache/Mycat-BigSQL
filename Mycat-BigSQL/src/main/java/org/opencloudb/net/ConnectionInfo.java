package org.opencloudb.net;

import java.net.InetSocketAddress;

import org.opencloudb.mysql.CharsetUtil;
import org.opencloudb.net.mysql.HandshakePacket;

public class ConnectionInfo {
	private long threadId;
	protected long id;
	protected String schema;
	protected String host;
	protected String localHost;
	protected int localPort;
	protected int port;
	private String user;
	private String password;
	private int charsetIndex;
	private String charset;
	private volatile int txIsolation;
	protected int packetHeaderSize;
	protected int maxPacketSize;
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected long netInBytes;
	protected long netOutBytes;
	private long idleTimeout;
	private byte[] seed;
	private HandshakePacket handshake;
	private long clientFlags;

	public String getHost() {
		return host;
	}

	public ConnectionInfo() {

	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getLocalPort() {
		return localPort;
	}

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getCharset() {
		return charset;
	}

	public int getPacketHeaderSize() {
		return packetHeaderSize;
	}

	public void setPacketHeaderSize(int packetHeaderSize) {
		this.packetHeaderSize = packetHeaderSize;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public void setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public String getLocalHost() {
		return localHost;
	}

	public void setLocalHost(String localHost) {
		this.localHost = localHost;
	}

	public void setStartupTime(long startupTime) {
		this.startupTime = startupTime;
	}

	public long getLastReadTime() {
		return lastReadTime;
	}

	public void setLastReadTime(long lastReadTime) {
		this.lastReadTime = lastReadTime;
	}

	public long getLastWriteTime() {
		return lastWriteTime;
	}

	public void setLastWriteTime(long lastWriteTime) {
		this.lastWriteTime = lastWriteTime;
	}

	public long getNetInBytes() {
		return netInBytes;
	}

	public void setNetInBytes(long netInBytes) {
		this.netInBytes = netInBytes;
	}

	public long getNetOutBytes() {
		return netOutBytes;
	}

	public void setNetOutBytes(long netOutBytes) {
		this.netOutBytes = netOutBytes;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public byte[] getSeed() {
		return seed;
	}

	public void setSeed(byte[] seed) {
		this.seed = seed;
	}

	public long getThreadId() {
		return threadId;
	}

	public void setThreadId(long threadId) {
		this.threadId = threadId;
	}

	public HandshakePacket getHandshake() {
		return handshake;
	}

	public void setHandshake(HandshakePacket handshake) {
		this.handshake = handshake;
	}

	public int getTxIsolation() {
		return txIsolation;
	}

	public void setTxIsolation(int txIsolation) {
		this.txIsolation = txIsolation;
	}

	public long getClientFlags() {
		return clientFlags;
	}

	public void setClientFlags(long clientFlags) {
		this.clientFlags = clientFlags;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getCharsetIndex() {
		return charsetIndex;
	}

	public String isSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public boolean setCharsetIndex(int ci) {
		String charset = CharsetUtil.getCharset(ci);
		if (charset != null) {
			return setCharset(charset);
		} else {
			return false;
		}
	}

	public boolean setCharset(String charset) {
		int ci = CharsetUtil.getIndex(charset);
		if (ci > 0) {
			this.charset = charset;
			this.charsetIndex = ci;
			return true;
		} else {
			return false;
		}
	}

	public String getSchema() {
		return schema;
	}

	public void setAddress(InetSocketAddress local, InetSocketAddress remote) {
		this.localHost = local.getHostString();
		this.localPort = local.getPort();
		this.host = remote.getHostString();
		this.port = remote.getPort();
	}

	@Override
	public String toString() {
		return "ConnectionInfo [threadId=" + threadId + ", id=" + id
				+ ", schema=" + schema + ", localHost=" + localHost + ":"
				+ localPort + "->" + host + ":" + port + ", user=" + user
				+ ", startupTime=" + startupTime + "]";
	}

}
