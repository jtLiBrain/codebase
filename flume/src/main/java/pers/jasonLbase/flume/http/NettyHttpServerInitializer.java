package pers.jasonLbase.flume.http;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.ipc.Responder;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ipfilter.IpFilterRule;
import io.netty.handler.ipfilter.RuleBasedIpFilter;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;

public class NettyHttpServerInitializer extends ChannelInitializer<SocketChannel> {
	private static int BYTES_K = 1024;
	private static int BYTES_M = 1024 * BYTES_K;
	
	private Responder responder;
	private AtomicLong connectNum;
	
	private SslContext sslCtx;
	private boolean enableCompression;
	private List<IpFilterRule> ipFilterRules;
	
	public NettyHttpServerInitializer(Responder responder, AtomicLong connectNum) {
		this.responder = responder;
		this.connectNum = connectNum;
	}
	
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
		
		if(sslCtx != null) {
			pipeline.addLast(sslCtx.newHandler(ch.alloc()));
		}
		
		if(ipFilterRules != null && !ipFilterRules.isEmpty()) {
			RuleBasedIpFilter ipFilterHandler = new RuleBasedIpFilter(ipFilterRules.toArray(new IpFilterRule[]{}));
			pipeline.addLast(ipFilterHandler);
		}
		
		pipeline.addLast(new HttpServerCodec());
		
		if(enableCompression) {
			pipeline.addLast(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.NONE));
			pipeline.addLast(ZlibCodecFactory.newZlibDecoder(ZlibWrapper.NONE));
			
			/*pipeline.addLast(new HttpContentCompressor());*/
		}
		
		pipeline.addLast(new HttpObjectAggregator(BYTES_M));
		pipeline.addLast(new ChunkedWriteHandler());
		
		pipeline.addLast(new NettyHttpServerAvroHandler(responder, connectNum));
	}

	/*============================================================================*
	 *                           getters and setters                              *
	 =============================================================================*/
	public SslContext getSslCtx() {
		return sslCtx;
	}
	
	public void setSslCtx(SslContext sslCtx) {
		this.sslCtx = sslCtx;
	}
	
	public boolean isEnableCompression() {
		return enableCompression;
	}

	public void setEnableCompression(boolean enableCompression) {
		this.enableCompression = enableCompression;
	}

	public List<IpFilterRule> getIpFilterRules() {
		return ipFilterRules;
	}

	public void setIpFilterRules(List<IpFilterRule> ipFilterRules) {
		this.ipFilterRules = ipFilterRules;
	}
}
