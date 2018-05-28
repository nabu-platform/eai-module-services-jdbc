package nabu.services.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.validation.constraints.NotNull;

import be.nabu.eai.api.Hidden;
import be.nabu.eai.module.services.jdbc.RepositoryDataSourceResolver;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.libs.artifacts.api.DataSourceProviderArtifact;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.NameProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import nabu.services.jdbc.types.Page;
import nabu.services.jdbc.types.Paging;
import nabu.services.jdbc.types.StoredProcedure;
import nabu.services.jdbc.types.StoredProcedureInterface;
import nabu.services.jdbc.types.Window;
import nabu.services.jdbc.types.StoredProcedureInterface.ParameterType;
import nabu.services.jdbc.types.StoredProcedureInterface.StoredProcedureParameter;

@WebService
public class Services {
	
	ExecutionContext executionContext;
	
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
				String schemaName = procedures.getString("PROCEDURE_SCHEM");
				String procedureName = procedures.getString("PROCEDURE_NAME");
				// apparently mssql does not support the SPECIFIC_NAME attribute
				// instead it throws an exception stating "The column name SPECIFIC_NAME is not valid"
				String specificName;
				try {
					specificName = procedures.getString("SPECIFIC_NAME");
				}
				catch (Exception e) {
					specificName = (schemaName == null ? "" : schemaName + ".") + procedureName;
				}
				String comments = null;
				try {
					comments = procedures.getString("REMARKS");
				}
				catch (Exception e) {
					// ignore
				}
				result.add(new StoredProcedure(
					procedures.getString("PROCEDURE_CAT"),
					schemaName,
					procedureName,
					comments,
					specificName
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
				String procedureName;
				try {
					procedureName = columns.getString("SPECIFIC_NAME");
				}
				catch (Exception e) {
					String procedureSchema = columns.getString("PROCEDURE_SCHEM");
					procedureName = columns.getString("PROCEDURE_NAME");
					if (procedureSchema != null) {
						procedureName = procedureSchema + "." + procedureName;
					}
				}
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
	
	@SuppressWarnings("unchecked")
	private Map<ComplexType, List<ComplexContent>> group(List<Object> instances) {
		Map<ComplexType, List<ComplexContent>> grouped = new HashMap<ComplexType, List<ComplexContent>>();
		for (Object instance : instances) {
			if (instance != null) {
				if (!(instance instanceof ComplexContent)) {
					instance = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(instance);
					if (instance == null) {
						throw new IllegalArgumentException("Can not be cast to complex content");
					}
				}
				ComplexType type = ((ComplexContent) instance).getType();
				if (!grouped.containsKey(type)) {
					grouped.put(type, new ArrayList<ComplexContent>());
				}
				grouped.get(type).add((ComplexContent) instance);
			}
		}
		return grouped;
	}
	
	private String generateInsert(ComplexType type) {
		StringBuilder sql = new StringBuilder();
		for (Element<?> child : TypeUtils.getAllChildren(type)) {
			if (!sql.toString().isEmpty()) {
				sql.append(",\n");
			}
			sql.append("\t" + child.getName());
		}
		return "insert into ~" + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + " (\n" + EAIRepositoryUtils.uncamelify(sql.toString()) + "\n) values (\n" + sql.toString().replaceAll("([\\w]+)", ":$1") + "\n)";
	}
	
	private String generateUpdate(ComplexType type) {
		StringBuilder sql = new StringBuilder();
		String idField = null;
		for (Element<?> child : TypeUtils.getAllChildren(type)) {
			Value<Boolean> property = child.getProperty(PrimaryKeyProperty.getInstance());
			if (property != null && property.getValue()) {
				idField = child.getName();
				continue;
			}
			if (!sql.toString().isEmpty()) {
				sql.append(",\n");
			}
			sql.append("\t" + EAIRepositoryUtils.uncamelify(child.getName()) + " = :" + child.getName());
		}
		if (idField == null) {
			throw new IllegalArgumentException("Could not determine primary key field");
		}
		return "update ~" + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + " set\n" + sql.toString() + "\n where " + (idField == null ? "<query>" : EAIRepositoryUtils.uncamelify(idField) + " = :" + idField);
	}
	
	private static String getName(Value<?>...properties) {
		String value = ValueUtils.getValue(CollectionNameProperty.getInstance(), properties);
		if (value == null) {
			value = ValueUtils.getValue(NameProperty.getInstance(), properties);
		}
		return value;
	}
	
	public void update(@WebParam(name = "connection") String connection, @WebParam(name = "transaction") String transaction, @WebParam(name = "instances") List<Object> instances) throws ServiceException {
		Map<ComplexType, List<ComplexContent>> group = group(instances);
		for (ComplexType type : group.keySet()) {
			List<ComplexContent> contents = group.get(type);
			String id = type instanceof DefinedType ? ((DefinedType) type).getId() : "$anonymous";
			id += ":generated.update";
			JDBCService jdbc = new JDBCService(id);
			jdbc.setDataSourceResolver(new RepositoryDataSourceResolver());
			jdbc.setInputGenerated(false);
			jdbc.setOutputGenerated(false);
			jdbc.setParameters(type);
			jdbc.setSql(generateUpdate(type));
			ComplexContent input = jdbc.getServiceInterface().getInputDefinition().newInstance();
			input.set(JDBCService.CONNECTION, connection);
			input.set(JDBCService.TRANSACTION, transaction);
			input.set(JDBCService.PARAMETERS, contents);
			ServiceRuntime runtime = new ServiceRuntime(jdbc, executionContext);
			runtime.run(input);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void insert(@WebParam(name = "connection") String connection, @WebParam(name = "transaction") String transaction, @WebParam(name = "instances") List<Object> instances) throws ServiceException {
		Map<ComplexType, List<ComplexContent>> group = group(instances);
		for (ComplexType type : group.keySet()) {
			List<ComplexContent> contents = group.get(type);
			String id = type instanceof DefinedType ? ((DefinedType) type).getId() : "$anonymous";
			id += ":generated.insert";
			JDBCService jdbc = new JDBCService(id);
			jdbc.setDataSourceResolver(new RepositoryDataSourceResolver());
			jdbc.setInputGenerated(false);
			jdbc.setOutputGenerated(false);
			jdbc.setParameters(type);
			jdbc.setSql(generateInsert(type));
			Element<?> primaryKey = null;
			// let's get the primary key to see if we have a generated column
			for (Element<?> child : TypeUtils.getAllChildren(type)) {
				Value<Boolean> property = child.getProperty(PrimaryKeyProperty.getInstance());
				if (property != null && property.getValue()) {
					primaryKey = child;
					Object primaryKeyValue = contents.get(0).get(child.getName());
					if (primaryKeyValue == null) {
						jdbc.setGeneratedColumn(child.getName());
					}
					break;
				}
			}
			ComplexContent input = jdbc.getServiceInterface().getInputDefinition().newInstance();
			input.set(JDBCService.CONNECTION, connection);
			input.set(JDBCService.TRANSACTION, transaction);
			input.set(JDBCService.PARAMETERS, contents);
			ServiceRuntime runtime = new ServiceRuntime(jdbc, executionContext);
			ComplexContent output = runtime.run(input);
			if (jdbc.getGeneratedColumn() != null) {
				List<Object> keys = (List<Object>) output.get(JDBCService.GENERATED_KEYS);
				int index = 0;
				for (ComplexContent content : contents) {
					Object currentValue = content.get(primaryKey.getName());
					if (currentValue == null && index < keys.size()) {
						content.set(primaryKey.getName(), keys.get(index++));
					}
				}
			}
		}
	}
	
	public void delete(@WebParam(name = "connection") String connection, @WebParam(name = "transaction") String transaction, @WebParam(name = "instances") List<Object> instances) throws ServiceException {
		Map<ComplexType, List<ComplexContent>> group = group(instances);
		for (ComplexType type : group.keySet()) {
			Element<?> primaryKey = null;
			for (Element<?> child : TypeUtils.getAllChildren(type)) {
				Value<Boolean> property = child.getProperty(PrimaryKeyProperty.getInstance());
				if (property != null && property.getValue()) {
					primaryKey = child;
					break;
				}
			}
			if (primaryKey == null) {
				throw new IllegalArgumentException("Could not find primary key");
			}
			
			List<ComplexContent> contents = group.get(type);
			String id = type instanceof DefinedType ? ((DefinedType) type).getId() : "$anonymous";
			id += ":generated.delete";
			JDBCService jdbc = new JDBCService(id);
			jdbc.setDataSourceResolver(new RepositoryDataSourceResolver());
			jdbc.setInputGenerated(false);
			jdbc.setOutputGenerated(false);
			jdbc.setParameters(type);
			jdbc.setSql("delete from ~" + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + " where " + EAIRepositoryUtils.uncamelify(primaryKey.getName()) + " = :" + primaryKey.getName());
			ComplexContent input = jdbc.getServiceInterface().getInputDefinition().newInstance();
			input.set(JDBCService.CONNECTION, connection);
			input.set(JDBCService.TRANSACTION, transaction);
			input.set(JDBCService.PARAMETERS, contents);
			ServiceRuntime runtime = new ServiceRuntime(jdbc, executionContext);
			runtime.run(input);
		}		
	}
}
