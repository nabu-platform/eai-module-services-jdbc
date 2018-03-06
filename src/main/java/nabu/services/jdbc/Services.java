package nabu.services.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.validation.constraints.NotNull;

import be.nabu.eai.api.Hidden;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.libs.artifacts.api.DataSourceProviderArtifact;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import nabu.services.jdbc.types.Page;
import nabu.services.jdbc.types.Paging;
import nabu.services.jdbc.types.StoredProcedure;
import nabu.services.jdbc.types.StoredProcedureInterface;
import nabu.services.jdbc.types.Window;
import nabu.services.jdbc.types.StoredProcedureInterface.ParameterType;
import nabu.services.jdbc.types.StoredProcedureInterface.StoredProcedureParameter;

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
	
	@WebResult(name = "page")
	public Window window(@WebParam(name = "limit") Integer limit, @WebParam(name = "offset") Long offset, @NotNull @WebParam(name = "rowCount") long rowCount, @WebParam(name = "hasNext") Boolean hasNext) {
		return Window.build(hasNext, rowCount, offset, limit);
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
	
	@Hidden
	@WebResult(name = "storedProcedures")
	public List<StoredProcedure> storedProcedures(@NotNull @WebParam(name = "connectionId") String connectionId, @WebParam(name = "catalogue") String catalogue, @WebParam(name = "schema") String schema, @WebParam(name = "name") String name) throws SQLException {
		DataSourceProviderArtifact artifact = (DataSourceProviderArtifact) EAIResourceRepository.getInstance().resolve(connectionId);
		if (artifact == null) {
			throw new IllegalArgumentException("Could not find: " + connectionId);
		}
		Connection connection = artifact.getDataSource().getConnection();
		try {
			List<StoredProcedure> result = new ArrayList<StoredProcedure>();
			DatabaseMetaData metaData = connection.getMetaData();
			ResultSet procedures = metaData.getProcedures(catalogue, schema, name);
			while (procedures.next()) {
				result.add(new StoredProcedure(
					procedures.getString("PROCEDURE_CAT"),
					procedures.getString("PROCEDURE_SCHEM"),
					procedures.getString("PROCEDURE_NAME"),
					procedures.getString("REMARKS"),
					procedures.getString("SPECIFIC_NAME")
				));
			}
			return result;
		}
		finally {
			connection.close();
		}
	}
	
	@Hidden
	@WebResult(name = "storedProcedureInterface")
	public StoredProcedureInterface storedProcedureInterface(@NotNull @WebParam(name = "connectionId") String connectionId, @WebParam(name = "catalogue") String catalogue, @WebParam(name = "schema") String schema, @WebParam(name = "name") String name, @WebParam(name = "uniqueName") String uniqueName) throws SQLException {
		DataSourceProviderArtifact artifact = (DataSourceProviderArtifact) EAIResourceRepository.getInstance().resolve(connectionId);
		if (artifact == null) {
			throw new IllegalArgumentException("Could not find: " + connectionId);
		}
		StoredProcedureInterface iface = new StoredProcedureInterface();
		Connection connection = artifact.getDataSource().getConnection();
		try {
			List<StoredProcedureParameter> parameters = new ArrayList<StoredProcedureParameter>();
			DatabaseMetaData metaData = connection.getMetaData();
			ResultSet columns = metaData.getProcedureColumns(catalogue, schema, name, null);
			while (columns.next()) {
				// multiple stored procedures can have the same name in the same namespace
				// but they have a different unique name
				// however, if you search for parameters based on the name, you get those for all the ones with the same name
				// we can filter on a specific name like this
				String procedureName = columns.getString("SPECIFIC_NAME");
				if (uniqueName != null && !uniqueName.equals(procedureName)) {
					continue;
				}
				StoredProcedureParameter parameter = new StoredProcedureParameter();
				parameter.setName(columns.getString("COLUMN_NAME"));
				parameter.setDescription(columns.getString("REMARKS"));
				parameter.setPrecision(columns.getInt("PRECISION"));
				parameter.setLength(columns.getInt("LENGTH"));
				parameter.setRadix(columns.getShort("RADIX"));
				parameter.setScale(columns.getShort("SCALE"));
				parameter.setParameterType(ParameterType.fromJdbcValue(columns.getShort("COLUMN_TYPE")));
				short nullable = columns.getShort("NULLABLE");
				if (nullable == DatabaseMetaData.procedureNoNulls) {
					parameter.setNullable(false);
				}
				else if (nullable == DatabaseMetaData.procedureNullable) {
					parameter.setNullable(true);
				}
				else {
					String isNullable = columns.getString("IS_NULLABLE");
					parameter.setNullable(isNullable != null && isNullable.equalsIgnoreCase("YES"));
				}
				parameter.setSqlType(columns.getInt("DATA_TYPE"));
				parameter.setProcedure(procedureName);
				parameters.add(parameter);
			}
			iface.setParameters(parameters);
		}
		finally {
			connection.close();
		}
		return iface;
	}
}
