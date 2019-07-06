package pers.jasonLbase.elk.es.search.scroll;

import java.io.IOException;

import com.alibaba.fastjson.JSONArray;

public interface ScrollQueryResultCallback {
	long process(JSONArray hitDocs) throws IOException;
}
