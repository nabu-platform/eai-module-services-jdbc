package nabu.services.jdbc.types;

import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder = { "current", "hasNext", "pageSize", "rowOffset" })
public class Window {
	
	private int current;
	private boolean hasNext;
	private long pageSize, rowOffset;

	public static Window build(Boolean hasNext, long rowCount, Long offset, Integer pageSize) {
		if (offset == null) {
			offset = 0l;
		}
		Window page = new Window();
		page.setRowOffset(offset);
		if (pageSize == null) {
			pageSize = (int) rowCount;
		}
		if (hasNext == null) {
			hasNext = rowCount == pageSize.longValue();
		}
		page.setPageSize(pageSize);
		page.setHasNext(hasNext);
		page.setCurrent(pageSize == null ? 0 : (int) Math.floor(((double) offset) / pageSize));
		return page;
	}
	
	public Window() {
		// auto
	}

	public int getCurrent() {
		return current;
	}

	public void setCurrent(int current) {
		this.current = current;
	}

	public long getPageSize() {
		return pageSize;
	}

	public void setPageSize(long pageSize) {
		this.pageSize = pageSize;
	}

	public long getRowOffset() {
		return rowOffset;
	}

	public void setRowOffset(long offset) {
		this.rowOffset = offset;
	}

	public boolean isHasNext() {
		return hasNext;
	}

	public void setHasNext(boolean hasNext) {
		this.hasNext = hasNext;
	}
	
}
