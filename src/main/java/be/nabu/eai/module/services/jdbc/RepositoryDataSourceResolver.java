package be.nabu.eai.module.services.jdbc;

import java.util.ArrayList;
import java.util.List;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceWrapper;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.services.jdbc.api.DynamicDataSourceResolver;

public class RepositoryDataSourceResolver implements DynamicDataSourceResolver {

	public static final String REGEX = System.getProperty("be.nabu.datasource.resolver", "^([^.]+)\\..*");
	public static final Boolean STRICT = Boolean.parseBoolean(System.getProperty("be.nabu.datasource.resolver.strict", "true"));
	
	@Override
	public String getDataSourceId(String forId) {
		ServiceRuntime runtime = ServiceRuntime.getRuntime();
		if (runtime != null) {
			Service service = runtime.getRoot().getService();
			if (service instanceof ServiceWrapper) {
				service = ((ServiceWrapper) service).getOriginal();
			}
			if (service instanceof DefinedService) {
				String toMatch = ((DefinedService) service).getId().replaceAll(REGEX, "$1");
				List<DataSourceWithDialectProviderArtifact> artifacts = EAIResourceRepository.getInstance().getArtifacts(DataSourceWithDialectProviderArtifact.class);
				List<DataSourceWithDialectProviderArtifact> hits = new ArrayList<DataSourceWithDialectProviderArtifact>();
				for (DataSourceWithDialectProviderArtifact artifact : artifacts) {
					String possible = artifact.getId().replaceAll(REGEX, "$1");
					if (toMatch.equals(possible)) {
						hits.add(artifact);
					}
				}
				if (hits.size() == 1 || (hits.size() > 1 && !STRICT)) {
					return hits.get(0).getId();
				}
			}
		}
		return null;
	}

}
