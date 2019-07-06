package pers.jasonLbase.elk.es.search.scroll;

import pers.jasonLbase.elk.es.search.SearchResult;

public class ScrollQueryResult extends SearchResult {
	private String scrollId;
	private long scrollHitsNum;

	public ScrollQueryResult() {
	}

	public ScrollQueryResult(String scrollId, long scrollHitsNum) {
		this.scrollId = scrollId;
		this.scrollHitsNum = scrollHitsNum;
	}

	public String getScrollId() {
		return scrollId;
	}

	public void setScrollId(String scrollId) {
		this.scrollId = scrollId;
	}

	public long getScrollHitsNum() {
		return scrollHitsNum;
	}

	public void setScrollHitsNum(long scrollHitsNum) {
		this.scrollHitsNum = scrollHitsNum;
	}
}
