package org.opencloudb;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.net.mysql.BackendMySQLProtocalHandler;
import org.opencloudb.net.mysql.FrontMysqlProtocalHandler;
import org.opencloudb.net.mysql.MySQLProtocalDecoder;

public class MycatStartup {
	private static final Logger LOGGER = Logger.getLogger("MycatBigSqlServer");

	public MycatStartup() {

		String home = SystemConfig.getHomePath();
		if (home == null) {
			System.out.println(SystemConfig.SYS_HOME + "  is not set.");
			System.exit(-1);
		}

	}

	public void run() throws Exception {
		SystemConfig sysConf = MycatSystem.getInstance().getConfig()
				.getSystem();
		int serverPort = sysConf.getServerPort();
		String serverIp = sysConf.getBindIp();
		int processorCount = sysConf.getProcessors();

		EventLoopGroup bossGroup = new NioEventLoopGroup(processorCount); // (1)
		EventLoopGroup workerGroup = bossGroup;
		try {
			ServerBootstrap b = new ServerBootstrap(); // (2)
			b.group(bossGroup, workerGroup)
					.channel(NioServerSocketChannel.class) // (3)
					.childHandler(new ChannelInitializer<SocketChannel>() { // (4)
								@Override
								public void initChannel(SocketChannel ch)
										throws Exception {
									ch.pipeline().addLast(
											new MySQLProtocalDecoder(),
											new FrontMysqlProtocalHandler());
								}
							}).option(ChannelOption.SO_BACKLOG, 128) // (5)
					.childOption(ChannelOption.SO_KEEPALIVE, true); // (6)
			MycatSystem.getInstance().getConfig().getSystem()
					.setSocketParams(b, true);
			// Bind and start to accept incoming connections.
			ChannelFuture f = b.bind(
					new InetSocketAddress(serverIp, serverPort)).sync(); // (7)

			Bootstrap cb = MycatSystem.getInstance().getSoketConnetor();
			cb.group(workerGroup).channel(NioSocketChannel.class)
					.handler(new ChannelInitializer<NioSocketChannel>() { // (4)
								@Override
								public void initChannel(NioSocketChannel ch)
										throws Exception {
									ch.pipeline().addLast(
											new MySQLProtocalDecoder(),
											new BackendMySQLProtocalHandler());
								}
							});
			MycatSystem.getInstance().getConfig().getSystem()
					.setSocketParams(cb, false);

			System.out.println("server started");
			// init datahost
			Map<String, PhysicalDBPool> dataHosts = MycatSystem.getInstance()
					.getConfig().getDataHosts();
			LOGGER.info("Initialize dataHost ...");
			for (PhysicalDBPool node : dataHosts.values()) {
				int index = 0;
				// String index = dnIndexProperties.getProperty(
				// node.getHostName(), "0");
				// if (!"0".equals(index)) {
				// LOGGER.info("init datahost: " + node.getHostName()
				// + "  to use datasource index:" + index);
				// }
				node.init(Integer.valueOf(index));
				// node.startHeartbeat();
			}
			System.out
					.println("MyCAT Server startup successfully. see logs in logs/mycat.log");
			// Wait until the server socket is closed.
			// In this example, this does not happen, but you can do that to
			// gracefully
			// shut down your server.

			f.channel().closeFuture().sync();
		} finally {
			System.out.println("stoped");
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

	public static void main(String[] args) throws Exception {
				new MycatStartup().run();
	}
}
