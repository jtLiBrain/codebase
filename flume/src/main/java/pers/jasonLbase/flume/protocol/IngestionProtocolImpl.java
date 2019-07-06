package pers.jasonLbase.flume.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.AvroRemoteException;
import org.apache.flume.Event;
import org.apache.flume.Source;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avro.protocol.journaldata.FGTEventIngestionRequest;
import avro.protocol.journaldata.FGTTrafficIngestionRequest;
import avro.protocol.journaldata.FGTUTMIncident;
import avro.protocol.journaldata.FGTUTMIngestionRequest;
import avro.protocol.journaldata.GenericEventIngestionRequest;
import avro.protocol.journaldata.GenericTrafficIngestionRequest;
import avro.protocol.journaldata.GenericUTMIngestionRequest;
import avro.protocol.journaldata.IngestionResponse;
import avro.protocol.journaldata.ResponseCode;
import avro.protocol.journaldata.ResponseStatus;
import pers.jasonLbase.avro.AvroUtil;

public class IngestionProtocolImpl implements IngestionProtocol {
	private static final Logger logger = LoggerFactory.getLogger(IngestionProtocolImpl.class);
	
	private Source flumeSource;

	public IngestionProtocolImpl(Source flumeSource) {
		this.flumeSource = flumeSource;
	}

	@Override
	public IngestionResponse fgtTrafficIngestion(FGTTrafficIngestionRequest fgtTrafficIngestionRequest)
			throws AvroRemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IngestionResponse fgtUTMIngestion(FGTEventIngestionRequest fgtEventIngestionRequest)
			throws AvroRemoteException {

		return null;
	}

	@Override
	public IngestionResponse fgtEventIngestion(FGTUTMIngestionRequest fgtUTMIngestionRequest) throws AvroRemoteException {
		List<Event> batch = new ArrayList<Event>();
		
		List<FGTUTMIncident> incidentList = fgtUTMIngestionRequest.getBody();
		for (FGTUTMIncident fgtutmIncident : incidentList) {
			try {
				Event event = EventBuilder.withBody(AvroUtil.encode(fgtutmIncident, FGTUTMIncident.SCHEMA$));
				batch.add(event);
			} catch (IOException e) {
				logger.error(null, e);
			}
		}
		
		ResponseStatus rs = new ResponseStatus();
		try {
			flumeSource.getChannelProcessor().processEventBatch(batch);
			rs.setResponseCode(ResponseCode.OK);
		} catch (Throwable t) {
			logger.error("IngestionProtocol: Unable to process event", t);
			rs.setResponseCode(ResponseCode.InternalServerError);
		}
		
		IngestionResponse response = new IngestionResponse();
		response.setResponseStatus(rs);

		return response;
	}

	@Override
	public IngestionResponse genericTrafficIngestion(GenericTrafficIngestionRequest genericTrafficIngestionRequest)
			throws AvroRemoteException {
		return null;
	}

	@Override
	public IngestionResponse genericEventIngestion(GenericEventIngestionRequest genericEventIngestionRequest)
			throws AvroRemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IngestionResponse genericUTMIngestion(GenericUTMIngestionRequest genericUTMIngestionRequest)
			throws AvroRemoteException {
		// TODO Auto-generated method stub
		return null;
	}

}
