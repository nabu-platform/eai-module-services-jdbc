package nabu.services.jdbc.types;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TypeDescription {
	private String collectionName, typeName;

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

}
