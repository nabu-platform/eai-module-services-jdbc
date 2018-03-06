package be.nabu.eai.module.services.jdbc.stored;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import nabu.services.jdbc.types.StoredProcedureInterface.StoredProcedureParameter;
import be.nabu.eai.repository.jaxb.ArtifactXMLAdapter;
import be.nabu.libs.artifacts.api.DataSourceProviderArtifact;

@XmlRootElement(name = "storedProcedure")
public class StoredProcedureConfiguration {
	private DataSourceProviderArtifact connection;
	private String catalogue, schema, name, uniqueName;
	private List<StoredProcedureParameter> parameters;
	
	@XmlJavaTypeAdapter(value = ArtifactXMLAdapter.class)
	public DataSourceProviderArtifact getConnection() {
		return connection;
	}
	public void setConnection(DataSourceProviderArtifact connection) {
		this.connection = connection;
	}
	
	public String getCatalogue() {
		return catalogue;
	}
	public void setCatalogue(String catalogue) {
		this.catalogue = catalogue;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getUniqueName() {
		return uniqueName;
	}
	public void setUniqueName(String uniqueName) {
		this.uniqueName = uniqueName;
	}
	public List<StoredProcedureParameter> getParameters() {
		return parameters;
	}
	public void setParameters(List<StoredProcedureParameter> parameters) {
		this.parameters = parameters;
	}
}
