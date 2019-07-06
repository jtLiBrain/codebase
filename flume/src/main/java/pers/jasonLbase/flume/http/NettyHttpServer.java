package pers.jasonLbase.flume.http;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.avro.ipc.Server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class NettyHttpServer implements Server {
	private static final Logger logger = LoggerFactory.getLogger(NettyHttpServer.class);

	private final Channel serverChannel;
	private final ServerBootstrap serverBootstrap;
	private final EventLoopGroup bossGroup;
	private final EventLoopGroup workerGroup;

	private final CountDownLatch closed = new CountDownLatch(1);

	public NettyHttpServer(InetSocketAddress addr, int workerThreads, ChannelInitializer<SocketChannel> initializer) throws InterruptedException {
		this.serverBootstrap = new ServerBootstrap();
		this.bossGroup = new NioEventLoopGroup(1);
		if(workerThreads > 0) {
			this.workerGroup = new NioEventLoopGroup(workerThreads);
		} else {
			this.workerGroup = new NioEventLoopGroup();
		}

		this.serverBootstrap.group(this.bossGroup, this.workerGroup)
		.channel(NioServerSocketChannel.class)
		/*.handler(new LoggingHandler(LogLevel.INFO))*/
		.childHandler(initializer);

		this.serverChannel = this.serverBootstrap.bind(addr).sync().channel();
	}
	
	@Override
	public void start() {
		// No-op.
	}

	@Override
	public void close() {
		try {
			this.serverChannel.closeFuture().sync();
		} catch (InterruptedException e) {
			logger.error(null, e);
		} finally {
			this.bossGroup.shutdownGracefully();
			this.workerGroup.shutdownGracefully();

			closed.countDown();
		}
	}

	@Override
	public int getPort() {
		return ((InetSocketAddress) this.serverChannel.localAddress()).getPort();
	}

	@Override
	public void join() throws InterruptedException {
		closed.await();
	}
}
