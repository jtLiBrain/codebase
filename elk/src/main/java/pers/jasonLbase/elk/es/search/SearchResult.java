package pers.jasonLbase.elk.es.search;

public class SearchResult {
	protected long hitsTotal;

	public SearchResult() {
	}

	public SearchResult(long hitsTotal) {
		this.hitsTotal = hitsTotal;
	}

	public long getHitsTotal() {
		return hitsTotal;
	}

	public void setHitsTotal(long hitsTotal) {
		this.hitsTotal = hitsTotal;
	}
}
