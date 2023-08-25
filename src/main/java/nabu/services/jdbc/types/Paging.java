package nabu.services.jdbc.types;

public class Paging {
	private Integer limit, offset, pageNumber;

	public Paging() {
		// auto construct
	}
	public Paging(Integer limit, Integer offset) {
		this.limit = limit;
		this.offset = offset;
	}
	public Integer getLimit() {
		return limit;
	}
	public void setLimit(Integer limit) {
		this.limit = limit;
	}
	public Integer getOffset() {
		return offset;
	}
	public void setOffset(Integer offset) {
		this.offset = offset;
	}
	public Integer getPageNumber() {
		if (pageNumber == null) {
			return limit != null && offset != null ? offset / limit : null;
		}
		return pageNumber;
	}
	public void setPageNumber(Integer pageNumber) {
		this.pageNumber = pageNumber;
	}
}
