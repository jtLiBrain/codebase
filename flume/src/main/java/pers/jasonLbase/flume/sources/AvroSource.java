package pers.jasonLbase.flume.sources;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLException;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pers.jasonLbase.flume.http.NettyHttpServer;
import pers.jasonLbase.flume.http.NettyHttpServerInitializer;
import pers.jasonLbase.flume.protocol.IngestionProtocol;
import pers.jasonLbase.flume.protocol.IngestionProtocolImpl;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ipfilter.IpFilterRule;
import io.netty.handler.ipfilter.IpFilterRuleType;
import io.netty.handler.ipfilter.IpSubnetFilterRule;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

public class AvroSource extends AbstractSource implements EventDrivenSource, Configurable {
	private static final Logger logger = LoggerFactory.getLogger(AvroSource.class);
	
	private static final String BIND_KEY = "bind";
	private static final String PORT_KEY = "port";
	
	private static final String THREADS_KEY = "threads";
	
	private static final String COMPRESSION_TYPE_KEY = "compression-type";
	
	private static final String SSL_KEY = "ssl";
	private static final String KEYCERTCHAINFILE_KEY = "keyCertChainFile";
	private static final String KEYFILE_KEY = "keyFile";
	private static final String KEYPASSWORD_KEY = "keyPassword";
	
	private static final String IP_FILTER_KEY = "ipFilter";
	private static final String IP_FILTER_RULES_KEY = "ipFilterRules";
	
	private Server server;
	private SourceCounter sourceCounter;
	
	private int maxThreads;
	private ScheduledExecutorService connectionCountUpdater;
	
	private int port;
	private String bindAddress;
	
	private String compressionType;
	
	private boolean enableSsl = false;
	private File keyCertChainFile;
	private File keyFile;
	private String keyPassword;
	private final List<String> excludeProtocols = new LinkedList<String>();
	
	private boolean enableIpFilter;
	private String patternRuleConfigDefinition;
	private List<IpFilterRule> rules;
	
	private volatile AtomicLong connectNum = new AtomicLong();
	
	/*============================================================================*
	 *                           Configurable                                     *
	 =============================================================================*/
	@Override
	public void configure(Context context) {
		Configurables.ensureRequiredNonNull(context, PORT_KEY, BIND_KEY);

		port = context.getInteger(PORT_KEY);
		bindAddress = context.getString(BIND_KEY);
		
		compressionType = context.getString(COMPRESSION_TYPE_KEY, "none");

		try {
			maxThreads = context.getInteger(THREADS_KEY, 0);
		} catch (NumberFormatException e) {
			logger.warn("AVRO source\'s \"threads\" property must specify an integer value.", context.getString(THREADS_KEY));
		}

		enableSsl = context.getBoolean(SSL_KEY, false);
		if (enableSsl) {
			String keyCertChainFileName = context.getString(KEYCERTCHAINFILE_KEY);
			String keyFileName = context.getString(KEYFILE_KEY);
			keyPassword = context.getString(KEYPASSWORD_KEY);
			
			keyCertChainFile = new File(keyCertChainFileName);
			if (!keyCertChainFile.exists() || keyCertChainFile.isDirectory()) {
				throw new FlumeException("Avro source configured with invalid keyCertChainFile: " + keyCertChainFileName);
			}
			
			keyFile = new File(keyFileName);
			if (!keyFile.exists() || keyFile.isDirectory()) {
				throw new FlumeException("Avro source configured with invalid keyFile: " + keyFileName);
			}
		}

		enableIpFilter = context.getBoolean(IP_FILTER_KEY, false);
		if (enableIpFilter) {
			patternRuleConfigDefinition = context.getString(IP_FILTER_RULES_KEY);
			if (patternRuleConfigDefinition == null || patternRuleConfigDefinition.trim().isEmpty()) {
				throw new FlumeException("ipFilter is configured with true but ipFilterRules is not defined:" + " ");
			}
			String[] patternRuleDefinitions = patternRuleConfigDefinition.split(",");
			rules = new ArrayList<IpFilterRule>(patternRuleDefinitions.length);
			for (String patternRuleDefinition : patternRuleDefinitions) {
				rules.add(generateRule(patternRuleDefinition));
			}
		}

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
	}

	private IpFilterRule generateRule(String patternRuleDefinition) throws FlumeException {
		patternRuleDefinition = patternRuleDefinition.trim();
		// first validate the format
		int firstColonIndex = patternRuleDefinition.indexOf(":");
		if (firstColonIndex == -1) {
			throw new FlumeException("Invalid ipFilter patternRule '" + patternRuleDefinition
					+ "' should look like <'allow'  or 'deny'>:<IPAddress>:<SubnetMask>");
		} else {
			String ruleAccessFlag = patternRuleDefinition.substring(0, firstColonIndex);
			
			int secondColonIndex = patternRuleDefinition.indexOf(":", firstColonIndex + 1);
			if ((!ruleAccessFlag.equals("allow") && !ruleAccessFlag.equals("deny")) || secondColonIndex == -1) {
				throw new FlumeException("Invalid ipFilter patternRule '" + patternRuleDefinition
						+ "' should look like <'allow'  or 'deny'>:<IPAddress>:<SubnetMask>");
			}

			String ipAddress = patternRuleDefinition.substring(firstColonIndex + 1, secondColonIndex);
			String subnetMask = patternRuleDefinition.substring(secondColonIndex + 1);
			int subnetMaskNum = 0;
			try {
				subnetMaskNum = Integer.valueOf(subnetMask);
			} catch(Exception e) {
				throw new FlumeException("Invalid ipFilter patternRule '" + patternRuleDefinition
						+ "' should look like <'allow'  or 'deny'>:<IPAddress>:<SubnetMask>");
			}
			
			boolean isAllow = ruleAccessFlag.equals("allow");
			
			IpFilterRuleType ruleType;
			if(isAllow) {
				ruleType = IpFilterRuleType.ACCEPT;
			} else {
				ruleType = IpFilterRuleType.REJECT;
			}
			
			return new IpSubnetFilterRule(ipAddress, subnetMaskNum, ruleType);
		}
	}
	
	/*============================================================================*
	 *                           AbstractSource                                     *
	 =============================================================================*/
	@Override
	public synchronized void start() {
		logger.info("Starting {}...", this);

		try {
			Responder responder = new SpecificResponder(IngestionProtocol.class, new IngestionProtocolImpl(this));
			
			ChannelInitializer<SocketChannel> initializer = initChannelInitializer(responder);
			
			server = new NettyHttpServer(new InetSocketAddress(bindAddress, port), maxThreads, initializer);
		} catch (Exception e) {
			logger.error("Avro source {} startup failed. Cannot initialize Netty server", getName(), e);
			stop();
			throw new FlumeException("Failed to set up server socket", e);
		}

		connectionCountUpdater = Executors.newSingleThreadScheduledExecutor();
		server.start();
		sourceCounter.start();
		super.start();
		connectionCountUpdater.scheduleWithFixedDelay(() -> sourceCounter.setOpenConnectionCount(connectNum.get()), 0, 60, TimeUnit.SECONDS);

		logger.info("Avro source {} started.", getName());
	}

	private ChannelInitializer<SocketChannel> initChannelInitializer(Responder responder) throws SSLException {
		NettyHttpServerInitializer initializer = new NettyHttpServerInitializer(responder, connectNum);
		
		initializer.setEnableCompression(compressionType.equalsIgnoreCase("deflate"));
		initializer.setIpFilterRules(rules);
		
		if(enableSsl) {
			SslContext sslCtx = SslContextBuilder.forServer(keyCertChainFile, keyFile, keyPassword).build();
			initializer.setSslCtx(sslCtx);
		}
		
		return initializer;
	}

	@Override
	public synchronized void stop() {
		logger.info("Avro source {} stopping: {}", getName(), this);

		if (server != null) {
			server.close();
			try {
				server.join();
				server = null;
			} catch (InterruptedException e) {
				logger.info("Avro source " + getName() + ": Interrupted while waiting "
						+ "for Avro server to stop. Exiting. Exception follows.", e);
				Thread.currentThread().interrupt();
			}
		}

		sourceCounter.stop();
		if (connectionCountUpdater != null) {
			connectionCountUpdater.shutdownNow();
			connectionCountUpdater = null;
		}

		super.stop();
		logger.info("Avro source {} stopped. Metrics: {}", getName(), sourceCounter);
	}

	@Override
	public String toString() {
		return "Avro source " + getName() + ": { bindAddress: " + bindAddress + ", port: " + port + " }";
	}
}
