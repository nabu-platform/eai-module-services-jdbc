package nabu.services.jdbc;

import java.util.ArrayList;
import java.util.List;

import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.validation.constraints.NotNull;

import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import nabu.services.jdbc.types.Page;
import nabu.services.jdbc.types.Paging;

@WebService
public class Services {
	@WebResult(name = "paging")
	public Paging paging(@WebParam(name = "limit") Integer limit, @WebParam(name = "maxLimit") @NotNull Integer maxLimit, @WebParam(name = "offset") Integer offset, @WebParam(name = "maxOffset") Integer maxOffset, @WebParam(name = "isPageOffset") Boolean isPageOffset) {
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
		if (isPageOffset == null || isPageOffset) {
			offset *= limit;
		}
		return new Paging(limit, offset);
	}
	
	@WebResult(name = "page")
	public Page page(@WebParam(name = "limit") Integer limit, @WebParam(name = "offset") Long offset, @WebParam(name = "totalRowCount") long totalRowCount) {
		return Page.build(totalRowCount, offset, limit);
	}
	
	@WebResult(name = "inserts")
	@SuppressWarnings("unchecked")
	public List<String> buildInserts(@WebParam(name = "instances") List<Object> objects, @NotNull @WebParam(name = "dialect") String dialect) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (objects == null || objects.isEmpty()) {
			return null;
		}
		SQLDialect result = (SQLDialect) Thread.currentThread().getContextClassLoader().loadClass(dialect).newInstance();
		List<String> inserts = new ArrayList<String>();
		for (Object object : objects) {
			if (object != null) {
				if (!(object instanceof ComplexContent)) {
					object = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(object);
				}
				if (object != null) {
					inserts.add(result.buildInsertSQL((ComplexContent) object));
				}
			}
		}
		return inserts;
	}
	
	@WebResult(name = "dialects")
	public List<String> dialects() {
		List<String> dialects = new ArrayList<String>();
		for (Class<?> implementation : EAIRepositoryUtils.getImplementationsFor(EAIResourceRepository.getInstance().getClassLoader(), SQLDialect.class)) {
			dialects.add(implementation.getName());
		}
		return dialects;
	}
}
