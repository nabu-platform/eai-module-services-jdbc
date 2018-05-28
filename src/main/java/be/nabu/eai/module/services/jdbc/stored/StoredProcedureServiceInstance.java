package be.nabu.eai.module.services.jdbc.stored;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import nabu.services.jdbc.types.StoredProcedureInterface.ParameterType;
import nabu.services.jdbc.types.StoredProcedureInterface.StoredProcedureParameter;
import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.converter.api.Converter;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceInstance;
import be.nabu.libs.services.api.Transactionable;
import be.nabu.libs.services.jdbc.DefaultDialect;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.services.jdbc.JDBCServiceInstance.ConnectionTransactionable;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.resultset.ResultSetCollectionHandler;
import be.nabu.libs.types.structure.StructureInstance;

public class StoredProcedureServiceInstance implements ServiceInstance {

	private StoredProcedureArtifact artifact;

	public StoredProcedureServiceInstance(StoredProcedureArtifact artifact) {
		this.artifact = artifact;
	}
	
	@Override
	public Service getDefinition() {
		return artifact;
	}

	@Override
	public ComplexContent execute(ExecutionContext executionContext, ComplexContent content) throws ServiceException {
		try {
			MetricInstance metrics = executionContext.getMetricInstance(artifact.getId());
			
			// get the connection id, you can override this at runtime. note that we don't need auto connection discovery (alla jdbc service) because you had to build the stored procedure from a connection
			String connectionId = content == null ? null : (String) content.get(JDBCService.CONNECTION);
			DataSourceWithDialectProviderArtifact dataSourceProvider;
			if (connectionId == null) {
				dataSourceProvider = (DataSourceWithDialectProviderArtifact) artifact.getConfig().getConnection();
				connectionId = dataSourceProvider.getId();
			}
			else {
				dataSourceProvider = executionContext.getServiceContext().getResolver(DataSourceWithDialectProviderArtifact.class).resolve(connectionId);
			}
			String transactionId = content == null ? null : (String) content.get(JDBCService.TRANSACTION);
			
			// if it's not autocommitted, we need to check if there is already a transaction open on this resource for the given transaction id
			Connection connection = null;
			try {
				if (!dataSourceProvider.isAutoCommit()) {
					// if there is no open transaction, create one
					Transactionable transactionable = executionContext.getTransactionContext().get(transactionId, connectionId);
					if (transactionable == null) {
						connection = dataSourceProvider.getDataSource().getConnection();
						executionContext.getTransactionContext().add(transactionId, new ConnectionTransactionable(connectionId, connection));
					}
					else {
						connection = ((ConnectionTransactionable) transactionable).getConnection();
					}
				}
				// it's autocommitted, just start a new connection
				else {
					connection = dataSourceProvider.getDataSource().getConnection();
				}
				
				SQLDialect dialect = dataSourceProvider.getDialect();
				if (dialect == null) {
					dialect = new DefaultDialect();
				}
				
				String sql = artifact.getSql();
				
				sql = artifact.getPreparedSql(dataSourceProvider.getDialect(), sql);
				
				CallableStatement statement = connection.prepareCall(sql);
				
				try {
					int counter = 1;
					
					ComplexContent parameters = content == null ? null : (ComplexContent) content.get(JDBCService.PARAMETERS);
		
					// if we have a return parameter, it is in the first place
					StoredProcedureParameter returnParameter = artifact.getReturnParameter();
					if (returnParameter != null) {
						statement.registerOutParameter(counter++, returnParameter.getSqlType());
					}
					
					if (artifact.getConfig().getParameters() != null) {
						ComplexType parametersInput = artifact.getParameters();
						for (StoredProcedureParameter parameter : artifact.getConfig().getParameters()) {
							if (parameter.getParameterType() == ParameterType.OUT || parameter.getParameterType() == ParameterType.IN_OUT) {
								statement.registerOutParameter(counter++, parameter.getSqlType());
							}
							if (parametersInput != null && (parameter.getParameterType() == ParameterType.IN_OUT || parameter.getParameterType() == ParameterType.IN)) {
								dialect.setObject(statement, parametersInput.get(StoredProcedureArtifact.cleanup(parameter.getName())), counter, parameters == null ? null : parameters.get(StoredProcedureArtifact.cleanup(parameter.getName())), sql);
								// if it is only in, we haven't increased the counter yet
								if (parameter.getParameterType() == ParameterType.IN) {
									counter++;
								}
							}
						}
					}
					
					ComplexContent output = artifact.getServiceInterface().getOutputDefinition().newInstance();
					
					if (artifact.hasResult()) {
						int index = 0;
						ResultSet executeQuery = statement.executeQuery();
						while (executeQuery.next()) {
							ComplexContent result;
							try {
								result = ResultSetCollectionHandler.convert(executeQuery, artifact.getResults());
							}
							catch (IllegalArgumentException e) {
								throw new ServiceException("JDBC-4", "Invalid type", e);
							}
							output.set(JDBCService.RESULTS + "[" + index++ + "]", result);
						}
					}
					else {
						statement.execute();
					}
					
					boolean setReturnValue = false;
					StructureInstance returnValue = artifact.getReturnValue().newInstance();
					int column = 1;
					if (returnParameter != null) {
						String cleanup = StoredProcedureArtifact.cleanup(returnParameter.getName());
						returnValue.set(cleanup, convert(statement.getObject(column++), returnValue.getType().get(cleanup)));
						setReturnValue = true;
					}
					for (StoredProcedureParameter parameter : artifact.getConfig().getParameters()) {
						if (parameter.getParameterType() == ParameterType.OUT || parameter.getParameterType() == ParameterType.IN_OUT) {
							String cleanup = StoredProcedureArtifact.cleanup(parameter.getName());
							returnValue.set(cleanup, convert(statement.getObject(column++), returnValue.getType().get(cleanup)));
							setReturnValue = true;
						}
						else {
							column++;
						}
					}
					
					if (setReturnValue) {
						output.set("return", returnValue);
					}
					
					return output;
				}
				finally {
					statement.close();
				}
			}
			finally {
				if (dataSourceProvider.isAutoCommit() && connection != null) {
					try {
						connection.close();
					}
					catch (SQLException e) {
						// do nothing
					}
				}
			}
		}
		catch (Exception e) {
			throw new ServiceException(e);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Object convert(Object value, Element<?> element) {
		if (value == null || !(element.getType() instanceof SimpleType)) {
			return value;
		}
		
		Converter converter = ConverterFactory.getInstance().getConverter();
		Class instanceClass = ((SimpleType) element.getType()).getInstanceClass();
		if (!converter.canConvert(value.getClass(), instanceClass)) {
			if (String.class.equals(((SimpleType) element.getType()).getInstanceClass())) {
				return value.toString();
			}
			else {
				throw new IllegalArgumentException("Can not convert " + value.getClass() + " to " + instanceClass);
			}
		}
		return converter.convert(value, instanceClass);
	}
	
}
