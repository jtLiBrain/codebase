package pers.jasonLbase.elk.es.doc;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.ParseException;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import pers.jasonLbase.elk.es.http.RestClientBase;

public class BulkClient extends RestClientBase {
	public BulkClient(HttpHost... hosts) {
		super(hosts);
	}
	
	public Response doPost(String index, String queryStr) throws IOException {
		String url = "/_bulk";
		
		if(index != null) {
			url = "/" + index + url;
		}
		
		Request request = new Request("POST", url);
		request.setJsonEntity(queryStr);
		
		return super.restClient.performRequest(request);
	}
	
	public String processResponse(Response response) throws ParseException, IOException {
		return EntityUtils.toString(response.getEntity());
	}
}
