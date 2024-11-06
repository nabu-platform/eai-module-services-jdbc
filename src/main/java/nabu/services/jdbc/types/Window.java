/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

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
