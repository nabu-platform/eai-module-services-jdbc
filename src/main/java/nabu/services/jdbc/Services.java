package nabu.services.jdbc;

import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;

import nabu.services.jdbc.types.Paging;

@WebService
public class Services {
	@WebResult(name = "paging")
	public Paging paging(@WebParam(name = "limit") Integer limit, @WebParam(name = "maxLimit") Integer maxLimit, @WebParam(name = "offset") Integer offset, @WebParam(name = "maxOffset") Integer maxOffset) {
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
