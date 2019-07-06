package pers.jasonLbase.flume.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.ipc.Responder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pers.jasonLbase.flume.utils.HttpTransceiver;
import pers.jasonLbase.flume.utils.HttpUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

public class NettyHttpServerAvroHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	private static final Logger logger = LoggerFactory.getLogger(NettyHttpServerAvroHandler.class);
	
	private Responder responder;
	private volatile AtomicLong connectNum;
	
	public NettyHttpServerAvroHandler(Responder responder, AtomicLong connectNum) {
		this.responder = responder;
		this.connectNum = connectNum;
	}
	
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws Exception {
		ByteBuf content = httpRequest.content();

		List<ByteBuffer> request = HttpUtil.readBuffers(content);
		List<ByteBuffer> response = responder.respond(request);
		// response will be null for oneway messages.
		if (response != null) {
			processResponse(ctx, response, httpRequest);
		}
	}

	private void processResponse(ChannelHandlerContext ctx, List<ByteBuffer> responseDatum, FullHttpRequest httpRequest) throws IOException {
		FullHttpResponse response = new DefaultFullHttpResponse(httpRequest.protocolVersion(), HttpResponseStatus.OK);
		
		// HTTP response header
		HttpHeaders headers = response.headers();
		headers.add(HttpHeaderNames.CONTENT_TYPE, HttpTransceiver.CONTENT_TYPE);
		
		boolean keepAlive = io.netty.handler.codec.http.HttpUtil.isKeepAlive(httpRequest);
		if(keepAlive) {
			headers.add(HttpHeaderNames.CONTENT_LENGTH, HttpTransceiver.getLength(responseDatum));
			headers.add(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
		}
		
		// HTTP response content
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		HttpTransceiver.writeBuffers(responseDatum, outStream);
		response.content().writeBytes(outStream.toByteArray());

		if(!keepAlive) {
			ctx.write(response).addListener(ChannelFutureListener.CLOSE);
		} else {
			ctx.write(response);
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		logger.warn("Unexpected exception from out-bound.", cause.getCause());
		ctx.close();
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		connectNum.incrementAndGet();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		connectNum.decrementAndGet();
	}
}
