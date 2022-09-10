package be.nabu.eai.module.services.jdbc;

import java.util.ArrayList;
import java.util.List;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.artifacts.api.ArtifactProxy;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.services.jdbc.api.DynamicDataSourceResolver;

// no longer necessary, we have encapsulated this logic into repositories
public class RepositoryDataSourceResolver implements DynamicDataSourceResolver {

	public static final String REGEX = System.getProperty("be.nabu.datasource.resolver", "^([^.]+)\\..*");
	public static final Boolean STRICT = Boolean.parseBoolean(System.getProperty("be.nabu.datasource.resolver.strict", "true"));
	
	@Override
	public String getDataSourceId(String forId) {
		DataSourceWithDialectProviderArtifact resolveFor = EAIResourceRepository.getInstance().resolveFor(forId, DataSourceWithDialectProviderArtifact.class);
		if (resolveFor instanceof ArtifactProxy)  {
			Artifact proxied = ((ArtifactProxy) resolveFor).getProxied();
			if (proxied instanceof DataSourceWithDialectProviderArtifact) {
				resolveFor = (DataSourceWithDialectProviderArtifact) proxied;
			}
		}
		return resolveFor == null ? null : resolveFor.getId();
		
//		ServiceRuntime runtime = ServiceRuntime.getRuntime();
//		if (runtime != null) {
//			List<DataSourceWithDialectProviderArtifact> artifacts = EAIResourceRepository.getInstance().getArtifacts(DataSourceWithDialectProviderArtifact.class);
//
//			// let's check if a pool has been explicitly configured for this artifact
//			String longest = getContextualFor(forId, artifacts);
//			if (longest != null) {
//				return longest;
//			}
//			
//			// we did not find for the explicit artifact, lets try the service context
//			String context = ServiceUtils.getServiceContext(runtime);
//			// let's check if a pool has been explicitly configured for this service context
//			longest = getContextualFor(context, artifacts);
//			if (longest != null) {
//				return longest;
//			}
//			
//			// get related hits for the service context
//			List<DataSourceWithDialectProviderArtifact> hits = getRelatedFor(context, artifacts);
//			
//			if (hits.isEmpty()) {
//				hits = getRelatedFor(forId, artifacts);
//			}
//
//			// if we have exactly one hit, return that
//			if (hits.size() == 1) {
//				return hits.get(0).getId();
//			}
//			// if we have multiple, do a match based on ids, the closest one wins
//			else if (hits.size() > 1) {
//				DataSourceWithDialectProviderArtifact closest = null;
//				int closestMatch = 0;
//				
//				String[] partsToMatch = forId.split("\\.");
//				for (DataSourceWithDialectProviderArtifact hit : hits) {
//					String[] parts = hit.getId().split("\\.");
//					int matchRate = 0;
//					for (int i = 0; i < Math.min(partsToMatch.length, parts.length); i++) {
//						if (partsToMatch[i].equals(parts[i])) {
//							matchRate++;
//						}
//						else {
//							break;
//						}
//					}
//					if (matchRate > closestMatch) {
//						closest = hit;
//					}
//				}
//				return closest == null ? null : closest.getId();
//			}
//		}
//		return null;
	}

	private String getContextualFor(String forId, List<DataSourceWithDialectProviderArtifact> artifacts) {
		String longest = null;
		String longestContext = null;
		for (DataSourceWithDialectProviderArtifact artifact : artifacts) {
			if (artifact.getContext() != null) {
				for (String context : artifact.getContext().split("[\\s]*,[\\s]*")) {
					if (forId.equals(context) || forId.startsWith(context + ".")) {
						if (longestContext == null || context.length() > longestContext.length()) {
							longestContext = context;
							longest = artifact.getId();
						}
					}
				}
			}
		}
		return longest;
	}
	
	private List<DataSourceWithDialectProviderArtifact> getRelatedFor(String forId, List<DataSourceWithDialectProviderArtifact> artifacts) {
		String longest = null;
		List<DataSourceWithDialectProviderArtifact> matches = new ArrayList<DataSourceWithDialectProviderArtifact>();
		for (DataSourceWithDialectProviderArtifact artifact : artifacts) {
			String id = artifact.getId();
			while (id != null) {
				if (forId.startsWith(id)) {
					if (longest == null || id.length() > longest.length()) {
						longest = id;
						matches.add(artifact);
					}
				}
				int index = id.lastIndexOf('.');
				id = index <= 0 ? null : id.substring(0, index);
			}
		}
		return matches;
	}

}
