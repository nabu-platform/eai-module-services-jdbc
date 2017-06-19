package nabu.services.jdbc.types;

import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder = { "current", "total", "pageSize", "rowOffset", "totalRowCount" })
public class Page {
	
	private int current, total;
	private long totalRowCount, pageSize, rowOffset;

	public static Page build(long totalAmount, Long offset, Integer pageSize) {
		if (offset == null) {
			offset = 0l;
		}
		Page page = new Page();
		page.setRowOffset(offset);
		page.setPageSize(pageSize == null ? totalAmount : pageSize);
		page.setTotalRowCount(totalAmount);
		page.setTotal(pageSize == null ? 1 : (int) Math.ceil(((double) totalAmount) / pageSize));
		page.setCurrent(pageSize == null ? 0 : (int) Math.floor(((double) offset) / pageSize));
		return page;
	}
	
	public Page() {
		// auto
	}

	public int getCurrent() {
		return current;
	}

	public void setCurrent(int current) {
		this.current = current;
	}

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public long getPageSize() {
		return pageSize;
	}

	public void setPageSize(long pageSize) {
		this.pageSize = pageSize;
	}

	public long getTotalRowCount() {
		return totalRowCount;
	}

	public void setTotalRowCount(long amount) {
		this.totalRowCount = amount;
	}

	public long getRowOffset() {
		return rowOffset;
	}

	public void setRowOffset(long offset) {
		this.rowOffset = offset;
	}
	
}
