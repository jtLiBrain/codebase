package pers.jasonLbase.flume.sources;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avro.protocol.journaldata.AgentDetails;
import avro.protocol.journaldata.FGTUTM;
import avro.protocol.journaldata.FGTUTMIncident;
import avro.protocol.journaldata.FGTUTMIngestionRequest;
import avro.protocol.journaldata.IngestionHeader;

public class AvroSourceTest {
	private static final Logger logger = LoggerFactory.getLogger(AvroSourceTest.class);

	private AvroSource source;
	private Channel channel;
	
	@Before
	public void setUp() throws UnknownHostException {
		source = new AvroSource();
		channel = new MemoryChannel();

		Configurables.configure(channel, new Context());

		List<Channel> channels = new ArrayList<Channel>();
		channels.add(channel);

		ChannelSelector rcs = new ReplicatingChannelSelector();
		rcs.setChannels(channels);

		source.setChannelProcessor(new ChannelProcessor(rcs));
	}

	@Test
	public void testStartAvroSource() throws InterruptedException, IOException {
		String port = "65111";
		String bind = "127.0.0.1";
		
		Context context = new Context();
		context.put("port", port);
		context.put("bind", bind);
		context.put("threads", "10");
		/*context.put("ipFilter", "true");
		context.put("ipFilterRules", "deny:127.0.0.1:24");*/
		
		boolean serverEnableCompression = false;
		
		if (serverEnableCompression) {
			context.put("compression-type", "deflate");
		} else {
			context.put("compression-type", "none");
		}

		Configurables.configure(source, context);

		source.start();

		/*IngestionProtocol client;
		Transceiver transceiver;
		if (clientEnableCompression) {
			transceiver = new HttpTransceiver(new URL("http://" + bind + ":" + port));

			client = SpecificRequestor.getClient(IngestionProtocol.class, transceiver);
		} else {
			transceiver = new HttpTransceiver(new URL("http://" + bind + ":" + port));

			client = SpecificRequestor.getClient(IngestionProtocol.class, transceiver);
		}

		IngestionResponse response = client.fgtEventIngestion(makeRequestDatum());

		Transaction transaction = channel.getTransaction();
		transaction.begin();

		Event event = channel.take();
		Assert.assertNotNull(event);
		Assert.assertEquals("Channel contained our event", "Hello avro", new String(event.getBody()));
		transaction.commit();
		transaction.close();

		logger.debug("Round trip event:{}", event);

		transceiver.close();*/
		
		TimeUnit.HOURS.sleep(1);
		
		source.stop();
		Assert.assertTrue("Reached stop or error", LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
		Assert.assertEquals("Server is stopped", LifecycleState.STOP, source.getLifecycleState());
	}
	
	private FGTUTMIngestionRequest makeRequestDatum() {
		FGTUTMIngestionRequest fgtUTMIngestionRequest = new FGTUTMIngestionRequest();
		
		// header
		IngestionHeader header = new IngestionHeader();
		header.setVersion(66L);
		header.setAuthToken("tst");
		header.setTenantId("1");
		header.setEventCategoryId("dd");
		header.setEventTypeId("1");
		header.setBatchTime(1L);
		
		AgentDetails ad = new AgentDetails();
		ad.setAgentId("1");
		
		header.setAgentDetails(ad);
		
		fgtUTMIngestionRequest.setHeader(header);
		
		// body
		List<FGTUTMIncident> bodys = new ArrayList<FGTUTMIncident>();
		
		FGTUTMIncident body1 = new FGTUTMIncident();
		FGTUTM utm = new FGTUTM();
		body1.setSelfFields(utm);
		
		bodys.add(body1);
		
		fgtUTMIngestionRequest.setBody(bodys);

		
		return fgtUTMIngestionRequest;
	}
}
