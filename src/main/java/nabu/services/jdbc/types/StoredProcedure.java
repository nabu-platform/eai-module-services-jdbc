package nabu.services.jdbc.types;

public class StoredProcedure {
	private String catalogue, schema, name, description, uniqueName;

	public StoredProcedure() {
		// auto
	}
	public StoredProcedure(String catalogue, String schema, String name, String description, String uniqueName) {
		this.catalogue = catalogue;
		this.schema = schema;
		this.name = name;
		this.description = description;
		this.uniqueName = uniqueName;
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
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getUniqueName() {
		return uniqueName;
	}
	public void setUniqueName(String uniqueName) {
		this.uniqueName = uniqueName;
	}
}
