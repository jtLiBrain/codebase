package pers.jasonLbase.elk.es.search;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import pers.jasonLbase.common.utils.FileUtils;
import pers.jasonLbase.elk.es.doc.BulkClient;
import pers.jasonLbase.elk.es.search.scroll.ScrollClient;
import pers.jasonLbase.elk.es.search.scroll.ScrollQueryResultCallback;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ES2ESPipline {
	public static void main(String[] args) throws IOException {
		InputStream in = ES2ESPipline.class.getResourceAsStream("/scroll.json");
		
		String queryStr = FileUtils.read(in, false);

		String index_from = "learn-catalog";
		String index_to = "learn-catalog2";
		
		HttpHost fromHost = new HttpHost("localhost", 9200, "http");
		HttpHost toHost = fromHost;
		
		ScrollClient scrollClient = new ScrollClient(fromHost);
		scrollClient.init();
		
		BulkClient bulkClient = new BulkClient(toHost);
		bulkClient.init();

		ScrollQueryResultCallback callback = new ScrollQueryResultCallback() {

			@Override
			public long process(JSONArray hitDocs) throws IOException {
				long scrollHitsNum = 0;
				
				StringBuilder sb = new StringBuilder();
				
				for (Iterator<Object> iter = hitDocs.iterator(); iter.hasNext();) {
					scrollHitsNum++;

					JSONObject hitDoc = (JSONObject) iter.next();
					
					String toBulk = hitDoc.getString("_source");
					
					String oneR = "{\"index\" : {}\n";
					sb.append(oneR)
					.append(toBulk + "\n");
				}

				if(scrollHitsNum > 0) {
					System.out.println(sb.toString());
					Response response = bulkClient.doPost(index_to, sb.toString());
					System.out.println(EntityUtils.toString(response.getEntity()));
				}
				
				return scrollHitsNum;
			}
		};

		try {
			scrollClient.doQuery(index_from, queryStr, callback);
		} finally {
			scrollClient.stop();
			bulkClient.stop();
		}
	}
}
