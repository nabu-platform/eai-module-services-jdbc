package nabu.services.jdbc.types;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TypeDescription {
	private String collectionName;

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
}
