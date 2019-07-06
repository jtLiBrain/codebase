package pers.jasonLbase.elk.es.search.scroll;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import pers.jasonLbase.elk.es.http.RestClientBase;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.net.HttpURLConnection;

public class ScrollClient extends RestClientBase {
	private String keepAlive = "1m";
	
	public ScrollClient(HttpHost... hosts) {
		super(hosts);
	}

	public void doQuery(String endpoin, String queryStr, ScrollQueryResultCallback callback) throws IOException {
		ScrollQueryResult scrollResult;
		String scrollId = null;
		long scrollHitsNum;

		try {
			scrollResult = doInitScrollRequest(endpoin, queryStr, callback);

			if (scrollResult != null) {
				scrollId = scrollResult.getScrollId();
				scrollHitsNum = scrollResult.getScrollHitsNum();

				while (scrollId != null && scrollHitsNum > 0) {
					scrollResult = doScrollRequest(scrollId, callback);
					
					if (scrollResult != null) {
						scrollId = scrollResult.getScrollId();
						scrollHitsNum = scrollResult.getScrollHitsNum();
					} else {
						scrollId = null;
						scrollHitsNum = 0;
					}
				}
			}
		} finally {
			clearScrollState(scrollId);
		}
	}

	private void clearScrollState(String scrollId) throws IOException {
		if (scrollId != null) {
			Request request = new Request("DELETE", "/_search/scroll");

			String payload = "{\"scroll_id\" : \"" + scrollId + "\"}";

			request.setJsonEntity(payload);

			Response response = super.restClient.performRequest(request);
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode != 200) {
				throw new IllegalStateException();
			}
		}
	}

	private ScrollQueryResult doInitScrollRequest(String index, String queryStr, ScrollQueryResultCallback callback)
			throws IOException {
		Request request = new Request("POST", "/" + index + "/_search?scroll=" + keepAlive);

		request.setJsonEntity(queryStr);

		Response response = super.restClient.performRequest(request);

		return processScrollResponse(response, callback);
	}

	private ScrollQueryResult doScrollRequest(String scrollId, ScrollQueryResultCallback callback) throws IOException {
		Request request = new Request("POST", "/_search/scroll");

		String payload = "{\"scroll\" : \"" + keepAlive + "\",\"scroll_id\" : \"" + scrollId + "\"}";

		request.setJsonEntity(payload);

		Response response = super.restClient.performRequest(request);

		return processScrollResponse(response, callback);
	}

	private ScrollQueryResult processScrollResponse(Response response, ScrollQueryResultCallback callback) throws IOException {
		int statusCode = response.getStatusLine().getStatusCode();
		
		if (statusCode == HttpURLConnection.HTTP_OK) {
			ScrollQueryResult ssr = new ScrollQueryResult();

			String responseBody = EntityUtils.toString(response.getEntity());
			
			JSONObject responseJsonO = JSON.parseObject(responseBody);

			String scrollId = responseJsonO.getString("_scroll_id");
			ssr.setScrollId(scrollId);

			long scrollHitsNum = 0;

			JSONObject hitsJsonO = responseJsonO.getJSONObject("hits");
			if (hitsJsonO != null) {
				long hitTotal = hitsJsonO.getJSONObject("total").getLongValue("value");

				if (hitTotal < 1) {
					return ssr;
				}
				
				JSONArray hitsJsonA = hitsJsonO.getJSONArray("hits");
				scrollHitsNum = callback.process(hitsJsonA);
			}

			ssr.setScrollHitsNum(scrollHitsNum);

			return ssr;
		} else {
			throw new IllegalStateException();
		}
	}
	
	public String getKeepAlive() {
		return keepAlive;
	}

	public void setKeepAlive(String keepAlive) {
		this.keepAlive = keepAlive;
	}
}