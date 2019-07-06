package pers.jasonLbase.flume.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;

import pers.jasonLbase.flume.protocol.IngestionProtocolImpl;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import pers.jasonLbase.flume.http.NettyHttpServer;
import pers.jasonLbase.flume.http.NettyHttpServerInitializer;
import pers.jasonLbase.flume.protocol.IngestionProtocol;

public class NettHttpServerTest {
	public static void main(String[] args) throws InterruptedException, IOException {
		Responder responder = new SpecificResponder(IngestionProtocol.class, new IngestionProtocolImpl(null));
		InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 65111);

		ChannelInitializer<SocketChannel> initializer = initChannelInitializer(responder);
		
		Server server = new NettyHttpServer(addr, 0, initializer);
		server.start();
		TimeUnit.MINUTES.sleep(30);

		server.close();
	}
	
	private static ChannelInitializer<SocketChannel> initChannelInitializer(Responder responder) {
		AtomicLong connectNum = new AtomicLong();
		NettyHttpServerInitializer initializer = new NettyHttpServerInitializer(responder, connectNum);
		return initializer;
	}
}
