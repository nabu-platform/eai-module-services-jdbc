package be.nabu.eai.module.services.jdbc;

import java.util.ArrayList;
import java.util.List;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.ServiceUtils;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.services.jdbc.api.DynamicDataSourceResolver;

public class RepositoryDataSourceResolver implements DynamicDataSourceResolver {

	public static final String REGEX = System.getProperty("be.nabu.datasource.resolver", "^([^.]+)\\..*");
	public static final Boolean STRICT = Boolean.parseBoolean(System.getProperty("be.nabu.datasource.resolver.strict", "true"));
	
	@Override
	public String getDataSourceId(String forId) {
		ServiceRuntime runtime = ServiceRuntime.getRuntime();
		if (runtime != null) {
			List<DataSourceWithDialectProviderArtifact> artifacts = EAIResourceRepository.getInstance().getArtifacts(DataSourceWithDialectProviderArtifact.class);

			// we search through the entire server, if a pool has been set up a context that matches, the longest match wins
			String longest = null;
			for (DataSourceWithDialectProviderArtifact artifact : artifacts) {
				if (artifact.getContext() != null && (forId.equals(artifact.getContext()) || forId.startsWith(artifact.getContext() + "."))) {
					if (longest == null || artifact.getContext().length() > longest.length()) {
						longest = artifact.getId();
					}
				}
			}
			if (longest != null) {
				return longest;
			}
			
			// we did not find a match based on explicit context, try implicit
			String context = ServiceUtils.getServiceContext(runtime);
			String toMatch = context.replaceAll(REGEX, "$1");
			List<DataSourceWithDialectProviderArtifact> hits = new ArrayList<DataSourceWithDialectProviderArtifact>();
			for (DataSourceWithDialectProviderArtifact artifact : artifacts) {
				String possible = artifact.getId().replaceAll(REGEX, "$1");
				if (toMatch.equals(possible)) {
					hits.add(artifact);
				}
			}
			// if we have exactly one hit, return that
			if (hits.size() == 1) {
				return hits.get(0).getId();
			}
			// if we have multiple, do a match based on ids, the closest one wins
			else if (hits.size() > 1) {
				DataSourceWithDialectProviderArtifact closest = null;
				int closestMatch = 0;
				
				String[] partsToMatch = forId.split("\\.");
				for (DataSourceWithDialectProviderArtifact hit : hits) {
					String[] parts = hit.getId().split("\\.");
					int matchRate = 0;
					for (int i = 0; i < Math.min(partsToMatch.length, parts.length); i++) {
						if (partsToMatch[i].equals(parts[i])) {
							matchRate++;
						}
						else {
							break;
						}
					}
					if (matchRate > closestMatch) {
						closest = hit;
					}
				}
				return closest == null ? null : closest.getId();
			}
		}
		return null;
	}

}
