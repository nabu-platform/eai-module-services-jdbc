package be.nabu.eai.module.services.jdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import be.nabu.eai.developer.api.InterfaceLister;
import be.nabu.eai.developer.util.InterfaceDescriptionImpl;

public class JDBCInterfaceLister implements InterfaceLister {

	private static Collection<InterfaceDescription> descriptions = null;
	
	@Override
	public Collection<InterfaceDescription> getInterfaces() {
		if (descriptions == null) {
			synchronized(JDBCInterfaceLister.class) {
				if (descriptions == null) {
					List<InterfaceDescription> descriptions = new ArrayList<InterfaceDescription>();
					descriptions.add(new InterfaceDescriptionImpl("JDBC Service", "Change Tracker", "be.nabu.libs.services.jdbc.api.ChangeTracker.track"));
					JDBCInterfaceLister.descriptions = descriptions;
				}
			}
		}
		return descriptions;
	}

}
