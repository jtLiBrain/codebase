package pers.jasonLbase.flume.sources;


import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

import avro.protocol.journaldata.AgentDetails;
import avro.protocol.journaldata.FGTUTM;
import avro.protocol.journaldata.FGTUTMIncident;
import avro.protocol.journaldata.FGTUTMIngestionRequest;
import avro.protocol.journaldata.IngestionHeader;
import avro.protocol.journaldata.IngestionResponse;

import pers.jasonLbase.flume.protocol.IngestionProtocol;

public class FortiClient {
	public static void main(String[] args) throws IOException {
		Transceiver transceiver = new HttpTransceiver(new URL("http://127.0.0.1:65111"));
		IngestionProtocol client = SpecificRequestor.getClient(IngestionProtocol.class, transceiver);
		
		FGTUTMIngestionRequest fgtUTMIngestionRequest = new FGTUTMIngestionRequest();
		
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
		
		List<FGTUTMIncident> bodys = new ArrayList<FGTUTMIncident>();
		
		FGTUTMIncident body1 = new FGTUTMIncident();
		
		FGTUTM utm = new FGTUTM();
		utm.setAction1("test action");
		body1.setSelfFields(utm);
		
		bodys.add(body1);
		
		fgtUTMIngestionRequest.setBody(bodys);
		
		IngestionResponse response = client.fgtEventIngestion(fgtUTMIngestionRequest);
		
		System.out.println(" client: " + response.toString());
		
		transceiver.close();
	}
}
