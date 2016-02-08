package nabu.services.jdbc;

import javax.jws.WebService;

import nabu.services.jdbc.types.Paging;

@WebService
public class Services {
	public Paging paging(Integer limit, Integer maxLimit, Integer offset, Integer maxOffset) {
		if (limit == null) {
			limit = maxLimit;
		}
		else if (limit < 0) {
			limit = maxLimit;
		}
		else if (maxLimit != null && limit > maxLimit) {
			limit = maxLimit;
		}
		if (offset == null || offset < 0) {
			offset = 0;
		}
		else if (maxOffset != null && offset > maxOffset) {
			offset = maxOffset;
		}
		return new Paging(limit, offset);
	}
}
