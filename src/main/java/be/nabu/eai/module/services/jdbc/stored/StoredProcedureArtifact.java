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

package be.nabu.eai.module.services.jdbc.stored;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nabu.services.jdbc.types.StoredProcedureInterface.ParameterType;
import nabu.services.jdbc.types.StoredProcedureInterface.StoredProcedureParameter;
import be.nabu.eai.api.NamingConvention;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.artifacts.jaxb.JAXBArtifact;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ServiceInstance;
import be.nabu.libs.services.api.ServiceInterface;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedSimpleType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleTypeWrapper;
import be.nabu.libs.types.base.ComplexElementImpl;
import be.nabu.libs.types.base.SimpleElementImpl;
import be.nabu.libs.types.base.TypeBaseUtils;
import be.nabu.libs.types.base.ValueImpl;
import be.nabu.libs.types.properties.CommentProperty;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.structure.DefinedStructure;
import be.nabu.libs.types.structure.Structure;

// TODO: the defined structures are overriden by build interface
// that means all customizations are always lost...
public class StoredProcedureArtifact extends JAXBArtifact<StoredProcedureConfiguration> implements DefinedService {

	private Structure input, output, combinedInput, returnValue, parameters, results;
	private SimpleTypeWrapper wrapper = SimpleTypeWrapperFactory.getInstance().getWrapper();
	private String sql;
	private Map<SQLDialect, Map<String, String>> preparedSql = new HashMap<SQLDialect, Map<String, String>>();
	
	public StoredProcedureArtifact(String id, ResourceContainer<?> directory, Repository repository) {
		super(id, directory, repository, "stored-procedure.xml", StoredProcedureConfiguration.class);
	}

	public static String cleanup(String name) {
		String cleaned = NamingConvention.LOWER_CAMEL_CASE.apply(name, NamingConvention.UNDERSCORE);
		if (!cleaned.substring(0, 1).matches("[a-z]+")) {
			cleaned = "arg" + cleaned;
		}
		return cleaned;
	}
	
	@Override
	public ServiceInterface getServiceInterface() {
		if (input == null || output == null) {
			buildInterface();
		}
		return new ServiceInterface() {
			@Override
			public ComplexType getInputDefinition() {
				return input;
			}
			@Override
			public ComplexType getOutputDefinition() {
				return output;
			}
			@Override
			public ServiceInterface getParent() {
				return null;
			}
		};
	}

	@Override
	public ServiceInstance newInstance() {
		return new StoredProcedureServiceInstance(this);
	}

	@Override
	public Set<String> getReferences() {
		return null;
	}
	
	public void buildInterface() {
		synchronized(this) {
			// instantiate if necessary
			if (input == null) {
				input = new Structure();
				input.setName("input");
			}
			if (output == null) {
				output = new Structure();
				output.setName("output");
			}
			if (combinedInput == null) {
				combinedInput = new Structure();
				combinedInput.setName("input");
			}
			
			Structure results = getResults();
			Structure parameters = getParameters();
			Structure returnValue = getReturnValue();
			
			// clear existing fields
			for (Element<?> child : TypeUtils.getAllChildren(input)) {
				input.remove(child);
			}
			for (Element<?> child : TypeUtils.getAllChildren(output)) {
				output.remove(child);
			}
			
			Map<String, Element<?>> parameterElements = new HashMap<String, Element<?>>();
			Map<String, Element<?>> resultElements = new HashMap<String, Element<?>>();
			Map<String, Element<?>> returnElements = new HashMap<String, Element<?>>();
			for (Element<?> child : TypeUtils.getAllChildren(parameters)) {
				parameterElements.put(child.getName(), child);
				parameters.remove(child);
			}
			for (Element<?> child : TypeUtils.getAllChildren(results)) {
				resultElements.put(child.getName(), child);
				results.remove(child);
			}
			for (Element<?> child : TypeUtils.getAllChildren(returnValue)) {
				returnElements.put(child.getName(), child);
				returnValue.remove(child);
			}
			
			List<StoredProcedureParameter> storedProcedureParameters = getConfig().getParameters();
			if (storedProcedureParameters != null) {
				for (StoredProcedureParameter parameter : storedProcedureParameters) {
					switch (parameter.getParameterType()) {
						case IN:
							if (!parameterElements.containsKey(cleanup(parameter.getName()))) {
								parameterElements.put(cleanup(parameter.getName()), create(parameter, parameters));
							}
							Element<?> element = parameterElements.get(cleanup(parameter.getName()));
							parameters.add(element);
							combinedInput.add(TypeBaseUtils.clone(element, combinedInput));
						break;
						case OUT:
							if (!returnElements.containsKey(cleanup(parameter.getName()))) {
								returnElements.put(cleanup(parameter.getName()), create(parameter, parameters));
							}
							element = returnElements.get(cleanup(parameter.getName()));
							returnValue.add(element);
							combinedInput.add(TypeBaseUtils.clone(element, combinedInput));
						break;
						case IN_OUT:
							if (!parameterElements.containsKey(cleanup(parameter.getName()))) {
								parameterElements.put(cleanup(parameter.getName()), create(parameter, parameters));
							}
							element = parameterElements.get(cleanup(parameter.getName()));
							parameters.add(element);

							if (!returnElements.containsKey(cleanup(parameter.getName()))) {
								returnElements.put(cleanup(parameter.getName()), create(parameter, parameters));
							}
							element = returnElements.get(cleanup(parameter.getName()));
							returnValue.add(element);
							
							// here we pick the input value for the data binding
							// note that input and output "can" diverge, but they must do so in a compatible way
							combinedInput.add(TypeBaseUtils.clone(element, combinedInput));
						break;
						case RESULT:
							if (!resultElements.containsKey(cleanup(parameter.getName()))) {
								resultElements.put(cleanup(parameter.getName()), create(parameter, parameters));
							}
							element = resultElements.get(cleanup(parameter.getName()));
							results.add(element);
						break;
						case RETURN:
							if (!returnElements.containsKey(cleanup(parameter.getName()))) {
								returnElements.put(cleanup(parameter.getName()), create(parameter, parameters));
							}
							element = returnElements.get(cleanup(parameter.getName()));
							returnValue.add(element);
						break;
						case UNKNOWN: 
							// we ignore unknown parameter types
						break;
					}
				}
			}
			input.add(new SimpleElementImpl<String>(JDBCService.CONNECTION, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
			input.add(new SimpleElementImpl<String>(JDBCService.TRANSACTION, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
			if (parameters.iterator().hasNext()) {
				input.add(new ComplexElementImpl(JDBCService.PARAMETERS, parameters, input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
			}
			if (results.iterator().hasNext()) {
				output.add(new ComplexElementImpl(JDBCService.RESULTS, results, output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
			}
			if (returnValue.iterator().hasNext()) {
				output.add(new ComplexElementImpl("return", returnValue, output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
			}
			sql = null;
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Element<?> create(StoredProcedureParameter parameter, ComplexType parent) {
		Class<?> type = getType(parameter.getSqlType());
		DefinedSimpleType<?> wrap = SimpleTypeWrapperFactory.getInstance().getWrapper().wrap(type);
		return new SimpleElementImpl(cleanup(parameter.getName()), wrap, parent, 
			new ValueImpl<Integer>(MinOccursProperty.getInstance(), parameter.isNullable() ? 0 : 1),
			new ValueImpl<String>(CommentProperty.getInstance(), parameter.getDescription()));
	}
	
	private Class<?> getType(int type) {
		switch (type) {
			case Types.INTEGER: return Integer.class;
			case Types.BIGINT: return BigInteger.class;
			case Types.BOOLEAN: return Boolean.class;
			case Types.BIT: return Byte.class;
			case Types.DECIMAL: return BigDecimal.class;
			case Types.DOUBLE: return Double.class;
			case Types.FLOAT: return Float.class;
			case Types.TINYINT: return Integer.class;
			case Types.SMALLINT: return Integer.class;
			case Types.NUMERIC: return BigDecimal.class;

			case Types.LONGVARBINARY: return byte[].class;
			case Types.BLOB: return byte[].class;
			case Types.BINARY: return byte[].class;
			case Types.VARBINARY: return byte[].class;
			
			case Types.DATE: 
			case Types.TIME_WITH_TIMEZONE:
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
			case Types.TIME: return Date.class;
			
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.LONGNVARCHAR:
			default: return java.lang.String.class;
		}
	}
	
	public boolean hasParameterType(ParameterType type) {
		if (getConfig().getParameters() != null) {
			for (StoredProcedureParameter parameter : getConfig().getParameters()) {
				if (parameter.getParameterType() == type) {
					return true;
				}
			}
		}
		return false;
	}
	
	boolean hasResult() {
		return hasParameterType(ParameterType.RESULT);
	}
	
	boolean hasParameters() {
		return hasParameterType(ParameterType.IN) || hasParameterType(ParameterType.IN_OUT);
	}
	
	boolean hasReturnValue() {
		return hasParameterType(ParameterType.RETURN) || hasParameterType(ParameterType.OUT) || hasParameterType(ParameterType.IN_OUT);
	}
	
	public StoredProcedureParameter getReturnParameter() {
		if (getConfig().getParameters() != null) {
			for (StoredProcedureParameter parameter : getConfig().getParameters()) {
				if (parameter.getParameterType() == ParameterType.RETURN) {
					return parameter;
				}
			}
		}
		return null;
	}
	
	public String getSql() {
		if (sql == null) {
			String sql = getReturnParameter() == null ? "{call " : "{? = call ";
			if (getConfig().getSchema() != null) {
				sql += getConfig().getSchema() + ".";
			}
			sql += getConfig().getName();
			sql += "(";
			if (getConfig().getParameters() != null) {
				boolean first = true;
				for (StoredProcedureParameter parameter : getConfig().getParameters()) {
					if (parameter.getParameterType() == ParameterType.RETURN || parameter.getParameterType() == ParameterType.RESULT || parameter.getParameterType() == ParameterType.UNKNOWN) {
						continue;
					}
					if (first) {
						first = false;
					}
					else {
						sql += ",";
					}
					sql += ":" + cleanup(parameter.getName());
				}
			}
			sql += ")}";
			this.sql = sql;
		}
		return sql;
	}

	Structure getParameters() {
		if (parameters == null) {
			parameters = new DefinedStructure();
			parameters.setName("parameters");
		}
		return parameters;
	}
	Structure getResults() {
		if (results == null) {
			results = new DefinedStructure();
			results.setName("results");
		}
		return results;
	}
	Structure getReturnValue() {
		if (returnValue == null) {
			returnValue = new DefinedStructure();
			returnValue.setName("return");
		}
		return returnValue;
	}

	String getPreparedSql(SQLDialect dialect, String sql) {
		if (!preparedSql.containsKey(dialect)) {
			synchronized(preparedSql) {
				if (!preparedSql.containsKey(dialect)) {
					preparedSql.put(dialect, new HashMap<String, String>());
				}
			}
		}
		if (!preparedSql.get(dialect).containsKey(sql)) {
			synchronized(preparedSql.get(dialect)) {
				if (!preparedSql.get(dialect).containsKey(sql)) {
					// we rewrite it with the combined input, it also contains the output for proper rewriting (?)
					preparedSql.get(dialect).put(sql, dialect == null ? sql : dialect.rewrite(sql, combinedInput, getResults()).replaceAll("(?<!:):[\\w]+", "?"));
				}
			}
		}
		return preparedSql.get(dialect).get(sql);
	}

	void setReturnValue(Structure returnValue) {
		this.returnValue = returnValue;
	}

	void setParameters(Structure parameters) {
		this.parameters = parameters;
	}

	void setResults(Structure results) {
		this.results = results;
	}
	
}
