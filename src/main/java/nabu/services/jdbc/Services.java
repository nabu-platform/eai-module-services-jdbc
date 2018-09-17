package nabu.services.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import be.nabu.eai.api.Hidden;
import be.nabu.eai.module.services.jdbc.JDBCServiceManager;
import be.nabu.eai.module.services.jdbc.RepositoryDataSourceResolver;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.artifacts.api.DataSourceProviderArtifact;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.services.jdbc.JDBCServiceInstance;
import be.nabu.libs.services.jdbc.api.ChangeTracker;
import be.nabu.libs.services.jdbc.api.DataSourceWithAffixes;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.services.jdbc.api.DataSourceWithAffixes.AffixMapping;
import be.nabu.libs.services.pojo.POJOUtils;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.KeyValuePair;
import be.nabu.libs.types.api.ModifiableElement;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.api.TypedKeyValuePair;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.HiddenProperty;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.properties.NameProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import nabu.services.jdbc.types.Page;
import nabu.services.jdbc.types.Paging;
import nabu.services.jdbc.types.StoredProcedure;
import nabu.services.jdbc.types.StoredProcedureInterface;
import nabu.services.jdbc.types.TypeDescription;
import nabu.services.jdbc.types.Window;
import nabu.services.jdbc.types.StoredProcedureInterface.ParameterType;
import nabu.services.jdbc.types.StoredProcedureInterface.StoredProcedureParameter;

// TODO: optimize the generated insert/update/select/delete adapters by persisting them (in non-dev mode)
// currently all the optimizations with regards to JDBC services are basically thrown out as we recreate them for each call
@WebService
public class Services {
	
	ExecutionContext executionContext;
	
	@XmlType(propOrder = { "entries", "sql" })
	@XmlRootElement
	public static class Explanation {
		private List<String> entries;
		private String sql;
		public List<String> getEntries() {
			return entries;
		}
		public void setEntries(List<String> entries) {
			this.entries = entries;
		}
		public String getSql() {
			return sql;
		}
		public void setSql(String sql) {
			this.sql = sql;
		}
	}
	
	@WebResult(name = "explanation")
	public Explanation explain(@NotNull @WebParam(name = "jdbcServiceId") String jdbcServiceId, @WebParam(name = "connectionId") String connectionId) throws SQLException {
		JDBCService service = (JDBCService) EAIResourceRepository.getInstance().resolve(jdbcServiceId);
		if (service == null) {
			throw new IllegalArgumentException("No jdbc service found with id: " + jdbcServiceId);
		}
		if (connectionId == null) {
			connectionId = service.getConnectionId();
		}
		if (connectionId == null) {
			connectionId = new RepositoryDataSourceResolver().getDataSourceId(jdbcServiceId);
		}
		if (connectionId == null) {
			throw new IllegalArgumentException("Can not find a valid connection id");
		}
		DataSourceWithDialectProviderArtifact provider = (DataSourceWithDialectProviderArtifact) EAIResourceRepository.getInstance().resolve(connectionId);
		if (provider == null) {
			throw new IllegalArgumentException("No connection found with id: " + connectionId);
		}
		
		String sql = service.getSql();
		// rewrite it if necessary
		if (provider.getDialect() != null) {
			sql = provider.getDialect().rewrite(sql, service.getParameters(), service.getResults());
		}
		
		List<AffixMapping> affixes = provider instanceof DataSourceWithAffixes ? ((DataSourceWithAffixes) provider).getAffixes() : null;
		sql = JDBCServiceInstance.replaceAffixes(service, affixes, sql);
		sql = sql.replaceAll("(?<!:):[\\w]+", "?");
		
		Explanation explanation = new Explanation();
		explanation.setSql(sql);
		
		Connection connection = provider.getDataSource().getConnection();
		try {
			PreparedStatement statement = connection.prepareStatement("explain " + sql);
			try {
				ComplexType type = (ComplexType) service.getInput().get(JDBCService.PARAMETERS).getType();
				int index = 1;
				for (String inputName : service.getInputNames()) {
					Element<?> child = type.get(inputName);
					// presumably dynamically fed in through properties
					if (child == null) {
						statement.setString(index++, "");
					}
					else if (child.getType() instanceof SimpleType) {
						if (child.getType().isList(child.getProperties()) && provider.getDialect().hasArraySupport(child)) {
							Class<?> instanceClass = ((SimpleType<?>) child.getType()).getInstanceClass();
							if (Integer.class.isAssignableFrom(instanceClass)) {
								statement.setObject(index++, new Integer[] { 1 });
							}
							else if (Long.class.isAssignableFrom(instanceClass)) {
								statement.setObject(index++, new Long[] { 1l });
							}
							else if (Float.class.isAssignableFrom(instanceClass)) {
								statement.setObject(index++, new Float[] { 1.0f });
							}
							else if (Double.class.isAssignableFrom(instanceClass)) {
								statement.setObject(index++, new Double[] { 1.0 });
							}
							else if (Boolean.class.isAssignableFrom(instanceClass)) {
								statement.setObject(index++, new Boolean[] { true });
							}
							else if (Date.class.isAssignableFrom(instanceClass)) {
								statement.setObject(index++, new Date[] { new java.sql.Date(new Date().getTime())});
							}
							else if (UUID.class.isAssignableFrom(instanceClass)) {
								statement.setObject(index++, new String[] { UUID.randomUUID().toString().replace("-", "") });
							}
							else {
								statement.setObject(index++, new String[] { "" });
							}
						}
						else {
							Class<?> instanceClass = ((SimpleType<?>) child.getType()).getInstanceClass();
							if (Integer.class.isAssignableFrom(instanceClass)) {
								statement.setInt(index++, 1);
							}
							else if (Long.class.isAssignableFrom(instanceClass)) {
								statement.setLong(index++, 1l);
							}
							else if (Float.class.isAssignableFrom(instanceClass)) {
								statement.setFloat(index++, 1.0f);
							}
							else if (Double.class.isAssignableFrom(instanceClass)) {
								statement.setDouble(index++, 1.0);
							}
							else if (Boolean.class.isAssignableFrom(instanceClass)) {
								statement.setBoolean(index++, true);
							}
							else if (Date.class.isAssignableFrom(instanceClass)) {
								statement.setDate(index++, new java.sql.Date(new Date().getTime()));
							}
							else if (UUID.class.isAssignableFrom(instanceClass)) {
								statement.setString(index++, UUID.randomUUID().toString().replace("-", ""));
							}
							else {
								statement.setString(index++, "");
							}
						}
					}
				}
				ResultSet executeQuery = statement.executeQuery();
				explanation.setEntries(new ArrayList<String>());
				while (executeQuery.next()) {
					explanation.getEntries().add(executeQuery.getString(1));
				}
				return explanation;
			}
			finally {
				statement.close();
			}
		}
		finally {
			connection.close();
		}
	}
	
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
	
	private String generateInsert(ComplexType type, boolean merge) {
		StringBuilder sql = new StringBuilder();
		String idField = null;
		for (Element<?> child : TypeUtils.getAllChildren(type)) {
			Value<Boolean> property = child.getProperty(PrimaryKeyProperty.getInstance());
			if (property != null && property.getValue()) {
				idField = child.getName();
			}
			if (!sql.toString().isEmpty()) {
				sql.append(",\n");
			}
			sql.append("\t" + child.getName());
		}
		String result = "insert into ~" + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + " (\n" + EAIRepositoryUtils.uncamelify(sql.toString()) + "\n) values (\n" + sql.toString().replaceAll("([\\w]+)", ":$1") + "\n)";
		if (merge) {
			if (idField == null) {
				throw new IllegalStateException("Can only auto merge if a primary key field is present");
			}
			result += "\non conflict(" + idField + ") do update set";
			result += "\n" + EAIRepositoryUtils.uncamelify(sql.toString()).replaceAll("([\\w]+)", "$1 = excluded.$1");
		}
		return result;
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
	
	@WebResult(name = "description")
	public TypeDescription describe(@WebParam(name = "typeId") String typeId) {
		ComplexType resolve = (ComplexType) DefinedTypeResolverFactory.getInstance().getResolver().resolve(typeId);
		TypeDescription description = new TypeDescription();
		description.setCollectionName(EAIRepositoryUtils.uncamelify(getName(resolve.getProperties())));
		return description;
	}
	
	private static String getName(Value<?>...properties) {
		String value = ValueUtils.getValue(CollectionNameProperty.getInstance(), properties);
		if (value == null) {
			value = ValueUtils.getValue(NameProperty.getInstance(), properties);
		}
		return value;
	}
	
	@SuppressWarnings("unchecked")
	@WebResult(name = "select")
	public JDBCSelectResult select(@WebParam(name = "connection") String connection, @WebParam(name = "transaction") String transaction, @NotNull @WebParam(name = "typeId") String typeId, @WebParam(name = "offset") Long offset, @WebParam(name = "limit") Integer limit, @WebParam(name = "orderBy") List<String> orderBy, @WebParam(name = "totalRowCount") Boolean totalRowCount, @WebParam(name = "hasNext") Boolean hasNext, @WebParam(name = "instanceId") Object id, @WebParam(name = "query") Object query) throws ServiceException {
		ComplexType resolve = (ComplexType) DefinedTypeResolverFactory.getInstance().getResolver().resolve(typeId);
		if (resolve == null) {
			throw new IllegalArgumentException("Could not find type: " + typeId);
		}
		List<ComplexType> types = new ArrayList<ComplexType>();
		ComplexType result = resolve;
		while (result != null) {
			types.add(result);
			result = (ComplexType) result.getSuperType();
		}
		Collections.reverse(types);
		
		Map<ComplexType, String> names = JDBCServiceManager.generateNames(types);
		StringBuilder from = new StringBuilder();
		StringBuilder where = new StringBuilder();
		ComplexType previous = null;
		Element<?> idField = null;
		// as we step through the types, we may encounter hidden types with their fields hidden
		// they belong to the next type in the line
		// keep track of them here and handle them in the first non-hidden type
		List<Element<?>> fields = new ArrayList<Element<?>>();
		Map<String, Element<?>> availableFields = new HashMap<String, Element<?>>();
		Map<String, String> fieldNames = new HashMap<String, String>();
		for (ComplexType type : types) {
			// always add all the elements
			for (Element<?> child : type) {
				fields.add(child);
			}
			Boolean value = ValueUtils.getValue(HiddenProperty.getInstance(), type.getProperties());
			if (value != null && value) {
				// add all fields to the hidden
				continue;
			}
			String typeName = EAIRepositoryUtils.uncamelify(getName(type.getProperties()));
			if (previous != null) {
				String previousName = names.get(previous);
				from.append(" join ~" + typeName + " " + names.get(type)).append(" on " + names.get(type) + ".id = " + previousName + ".id");
			}
			else {
				from.append(" ~").append(typeName + " " + names.get(type));
			}
			if ((id != null && idField == null) || query != null) {
				for (Element<?> child : fields) {
					if (id != null && idField == null) {
						Value<Boolean> property = child.getProperty(PrimaryKeyProperty.getInstance());
						if (property != null && property.getValue()) {
							idField = child;
							where.append(" where " + names.get(type) + "." + EAIRepositoryUtils.uncamelify(child.getName()) + " = :id");
						}
					}
					if (query != null) {
						availableFields.put(child.getName(), child);
						fieldNames.put(child.getName(), names.get(type) + "." + EAIRepositoryUtils.uncamelify(child.getName()));
					}
				}
				if (id != null && idField == null) {
					throw new IllegalArgumentException("Could not find id field for type " + type + " (id: " + id + ")");
				}
			}
			previous = type;
			// clear the fields before we start on the next type
			fields.clear();
		}
		
		String sql = "select * from " + from.toString();
		
		List<Element<?>> queryParameters = new ArrayList<Element<?>>();
		if (query != null) {
			if (!(query instanceof ComplexContent)) {
				query = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(query);
			}
			for (Element<?> child : TypeUtils.getAllChildren(((ComplexContent) query).getType())) {
				// if the key exists in the record, let's search it
				// if we have an id field, don't search it unless we don't provide an instance id as input
				if (availableFields.containsKey(child.getName()) && (idField == null || !idField.getName().equals(child.getName()))) {
					Object object = ((ComplexContent) query).get(child.getName());
					if (object == null) {
						Value<Integer> property = child.getProperty(MinOccursProperty.getInstance());
						// if it is a required property but has no value, we want to find the values that are null
						if (property == null || property.getValue() >= 1) {
							if (where.length() == 0) {
								where.append(" where ");
							}
							else {
								where.append(" and ");
							}
							where.append(fieldNames.get(child.getName()) + " is null");
							queryParameters.add(child);
						}
						// otherwise just an optional query parameter, skip it
						else {
							continue;
						}
					}
					else {
						Value<Integer> maxOccurs = child.getProperty(MaxOccursProperty.getInstance());
						if (where.length() == 0) {
							where.append(" where ");
						}
						else {
							where.append(" and ");
						}
						String fieldName = fieldNames.get(child.getName());
						queryParameters.add(child);
						if (maxOccurs == null || maxOccurs.getValue() == 1) {
							Element<?> available = availableFields.get(child.getName());
							// if it is a single string, do a contains
							if (available.getType() instanceof SimpleType && String.class.isAssignableFrom(((SimpleType<?>) available.getType()).getInstanceClass())) {
								where.append("lower(" + fieldName + ")")
									.append(" like '%' || lower(:" + child.getName() + ") || '%'");
							}
							else {
								where.append(fieldName)
									.append(" = :"+ child.getName());
							}
						}
						else {
							where.append(fieldName)
								.append(" = any(:" + child.getName() + ")");
						}
					}
				}
			}
		}
		sql += where.toString();
		
		String serviceId = typeId + ":generated.select" + (idField == null ? "" : "ById");
		JDBCService jdbc = new JDBCService(serviceId);
		jdbc.setDataSourceResolver(new RepositoryDataSourceResolver());
		jdbc.setInputGenerated(true);
		jdbc.setOutputGenerated(false);
		jdbc.setResults(resolve);
		jdbc.setSql(sql);
		
		// we have regenerated the input, now set the correct input type
		if (idField != null) {
			Element<?> element = jdbc.getParameters().get("id");
			((ModifiableElement<?>) element).setType(idField.getType());
		}
		
		for (Element<?> queryParameter : queryParameters) {
			Element<?> element = jdbc.getParameters().get(queryParameter.getName());
			// get the type as defined in the structure
			((ModifiableElement<?>) element).setType(availableFields.get(queryParameter.getName()).getType());
			// get list properties from the query
			Value<Integer> maxOccurs = queryParameter.getProperty(MaxOccursProperty.getInstance());
			Value<Integer> minOccurs = queryParameter.getProperty(MinOccursProperty.getInstance());
			if (maxOccurs != null) {
				element.setProperty(maxOccurs);
			}
			if (minOccurs != null) {
				element.setProperty(minOccurs);
			}
		}
		
		// create a new instance of the parameters so we can set the (optional) id
		ComplexContent parameters = jdbc.getParameters().newInstance();
		if (idField != null) {
			parameters.set(idField.getName(), id);
		}
		
		for (Element<?> queryParameter : queryParameters) {
			parameters.set(queryParameter.getName(), ((ComplexContent) query).get(queryParameter.getName()));
		}
		
		ComplexContent input = jdbc.getServiceInterface().getInputDefinition().newInstance();
		input.set(JDBCService.CONNECTION, connection);
		input.set(JDBCService.TRANSACTION, transaction);
		input.set(JDBCService.PARAMETERS, parameters);
		input.set(JDBCService.LIMIT, limit);
		input.set(JDBCService.OFFSET, offset);
		input.set(JDBCService.ORDER_BY, orderBy);
		input.set(JDBCService.TOTAL_ROW_COUNT, totalRowCount);
		input.set(JDBCService.HAS_NEXT, hasNext);
		ServiceRuntime runtime = new ServiceRuntime(jdbc, executionContext);
		ComplexContent output = runtime.run(input);
		
		return new JDBCSelectResult(
			(List<Object>) output.get(JDBCService.RESULTS), 
			(Long) output.get(JDBCService.ROW_COUNT), 
			(Long) output.get(JDBCService.TOTAL_ROW_COUNT), 
			(Boolean) output.get(JDBCService.HAS_NEXT)
		);
	}
	
	@SuppressWarnings("unchecked")
	public JDBCSelectResult selectDynamic(@WebParam(name = "connection") String connection, @WebParam(name = "transaction") String transaction, @WebParam(name = "typeId") String typeId, @WebParam(name = "offset") Long offset, @WebParam(name = "limit") Integer limit, @WebParam(name = "orderBy") List<String> orderBy, @WebParam(name = "totalRowCount") Boolean totalRowCount, @WebParam(name = "hasNext") Boolean hasNext, @WebParam(name = "instanceId") Object id, @WebParam(name = "sql") String sql, @WebParam(name = "properties") List<KeyValuePair> properties) throws ServiceException {
		String serviceId = typeId + ":generated.selectDynamic";
		JDBCService jdbc = new JDBCService(serviceId);
		jdbc.setDataSourceResolver(new RepositoryDataSourceResolver());
		jdbc.setInputGenerated(true);
		jdbc.setOutputGenerated(typeId == null);
		// if we don't have a type, a new one will be generated and used
		// the results can be used in dynamic scenarios
		if (typeId != null) {
			ComplexType resolve = (ComplexType) DefinedTypeResolverFactory.getInstance().getResolver().resolve(typeId);
			if (resolve == null) {
				throw new IllegalArgumentException("Could not find type: " + typeId);
			}
			jdbc.setResults(resolve);
		}
		
		// triggers generation of input/output
		jdbc.setSql(sql);
		
		Map<String, SimpleType<?>> types = new HashMap<String, SimpleType<?>>();
		if (properties != null) {
			for (KeyValuePair pair : properties) {
				if (pair instanceof TypedKeyValuePair) {
					String type = ((TypedKeyValuePair) pair).getType();
					SimpleType<?> simpleType = null;
					if (type != null) {
						simpleType = SimpleTypeWrapperFactory.getInstance().getWrapper().getByName(type);
					}
					if (simpleType == null) {
						simpleType = SimpleTypeWrapperFactory.getInstance().getWrapper().wrap(String.class);
					}
					types.put(pair.getKey(), simpleType);
				}
			}
		}
		// an input document was generated, we just need to set the correct types
		for (Element<?> element : TypeUtils.getAllChildren(jdbc.getParameters())) {
			if (element instanceof ModifiableElement) {
				if (types.containsKey(element.getName())) {
					((ModifiableElement<?>) element).setType(types.get(element.getName()));
				}
			}
		}
		
		ComplexContent newInstance = jdbc.getParameters().newInstance();
		if (properties != null) {
			for (KeyValuePair pair : properties) {
				newInstance.set(pair.getKey(), pair.getValue());
			}
		}
		
		ComplexContent input = jdbc.getServiceInterface().getInputDefinition().newInstance();
		input.set(JDBCService.CONNECTION, connection);
		input.set(JDBCService.TRANSACTION, transaction);
		input.set(JDBCService.PARAMETERS, newInstance);
		input.set(JDBCService.LIMIT, limit);
		input.set(JDBCService.OFFSET, offset);
		input.set(JDBCService.ORDER_BY, orderBy);
		input.set(JDBCService.TOTAL_ROW_COUNT, totalRowCount);
		input.set(JDBCService.HAS_NEXT, hasNext);
		ServiceRuntime runtime = new ServiceRuntime(jdbc, executionContext);
		ComplexContent output = runtime.run(input);
		
		return new JDBCSelectResult(
			(List<Object>) output.get(JDBCService.RESULTS), 
			(Long) output.get(JDBCService.ROW_COUNT), 
			(Long) output.get(JDBCService.TOTAL_ROW_COUNT), 
			(Boolean) output.get(JDBCService.HAS_NEXT)
		);
	}
	
	public static class JDBCSelectResult {
		private List<Object> results;
		private Long rowCount, totalRowCount;
		private Boolean hasNext;
		public JDBCSelectResult() {
			// auto
		}
		public JDBCSelectResult(List<Object> results, Long rowCount, Long totalRowCount, Boolean hasNext) {
			this.results = results;
			this.rowCount = rowCount;
			this.totalRowCount = totalRowCount;
			this.hasNext = hasNext;
		}
		public Long getTotalRowCount() {
			return totalRowCount;
		}
		public void setTotalRowCount(Long totalRowCount) {
			this.totalRowCount = totalRowCount;
		}
		public Long getRowCount() {
			return rowCount;
		}
		public void setRowCount(Long rowCount) {
			this.rowCount = rowCount;
		}
		public Boolean isHasNext() {
			return hasNext;
		}
		public void setHasNext(Boolean hasNext) {
			this.hasNext = hasNext;
		}
		public List<Object> getResults() {
			return results;
		}
		public void setResults(List<Object> results) {
			this.results = results;
		}
	}
	
	public void update(@WebParam(name = "connection") String connection, @WebParam(name = "transaction") String transaction, @WebParam(name = "instances") List<Object> instances, @WebParam(name = "changeTracker") String changeTracker) throws ServiceException {
		Map<ComplexType, List<ComplexContent>> group = group(instances);
		for (ComplexType type : group.keySet()) {
			List<ComplexContent> contents = group.get(type);
			String id = type instanceof DefinedType ? ((DefinedType) type).getId() : "$anonymous";
			id += ":generated.update";
			JDBCService jdbc = new JDBCService(id);
			jdbc.setChangeTracker(toChangeTracker(changeTracker));
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
	
	private ChangeTracker toChangeTracker(String id) {
		if (id == null) {
			return null;
		}
		Artifact resolve = EAIResourceRepository.getInstance().resolve(id);
		if (resolve instanceof Service) {
			return POJOUtils.newProxy(ChangeTracker.class, EAIResourceRepository.getInstance(), SystemPrincipal.ROOT, (Service) resolve);
		}
		else {
			throw new IllegalArgumentException("The artifact is not a change tracker: " + id);
		}
	}
	
	public void insert(@WebParam(name = "connection") String connection, @WebParam(name = "transaction") String transaction, @WebParam(name = "instances") List<Object> instances, @WebParam(name = "changeTracker") String changeTracker) throws ServiceException {
		insertOrUpdate(connection, transaction, instances, changeTracker, false);
	}
	
	public void merge(@WebParam(name = "connection") String connection, @WebParam(name = "transaction") String transaction, @WebParam(name = "instances") List<Object> instances, @WebParam(name = "changeTracker") String changeTracker) throws ServiceException {
		insertOrUpdate(connection, transaction, instances, changeTracker, true);
	}

	@SuppressWarnings("unchecked")
	private void insertOrUpdate(String connection, String transaction, List<Object> instances, String changeTracker, boolean merge) throws ServiceException {
		Map<ComplexType, List<ComplexContent>> group = group(instances);
		for (ComplexType type : group.keySet()) {
			List<ComplexContent> contents = group.get(type);
			String id = type instanceof DefinedType ? ((DefinedType) type).getId() : "$anonymous";
			id += ":generated." + (merge ? "merge" : "insert");
			JDBCService jdbc = new JDBCService(id);
			jdbc.setChangeTracker(toChangeTracker(changeTracker));
			jdbc.setDataSourceResolver(new RepositoryDataSourceResolver());
			jdbc.setInputGenerated(false);
			jdbc.setOutputGenerated(false);
			jdbc.setParameters(type);
			jdbc.setSql(generateInsert(type, merge));
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
	
	public void delete(@WebParam(name = "connection") String connection, @WebParam(name = "transaction") String transaction, @WebParam(name = "instances") List<Object> instances, @WebParam(name = "changeTracker") String changeTracker) throws ServiceException {
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
			jdbc.setChangeTracker(toChangeTracker(changeTracker));
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
