package pers.jasonLbase.elk.es.http;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;

public abstract class RestClientBase {
	protected HttpHost[] hosts;
	protected RestClient restClient;

	protected RestClientBase(HttpHost... hosts) {
		this.hosts = hosts;
	}
	
	public void init() {
		RestClientBuilder builder = RestClient.builder(hosts);

		restClient = builder.build();
	}

	public void stop() throws IOException {
		if (restClient != null)
			restClient.close();
	}

	public HttpHost[] getHosts() {
		return hosts;
	}

	public void setHosts(HttpHost[] hosts) {
		this.hosts = hosts;
	}

	public RestClient getRestClient() {
		return restClient;
	}

	public void setRestClient(RestClient restClient) {
		this.restClient = restClient;
	}
}
