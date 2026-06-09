package be.nabu.eai.module.services.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import be.nabu.eai.module.types.structure.StructureManager;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.CreatableArtifactFragmentManager;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.ResourceEntry;
import be.nabu.eai.repository.impl.DefinedServiceArtifactFragmentManager;
import be.nabu.eai.repository.resources.RepositoryEntry;
import be.nabu.libs.services.DefinedServiceInterfaceResolverFactory;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.DefinedServiceInterface;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.libs.types.definition.xml.XMLDefinitionMarshaller;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.services.pojo.POJOUtils;
import be.nabu.libs.types.java.BeanResolver;
import be.nabu.libs.types.structure.DefinedStructure;
import be.nabu.libs.types.structure.Structure;
import be.nabu.libs.validator.api.Validation;
import be.nabu.libs.validator.api.ValidationMessage;

public class JDBCServiceArtifactFragmentManager extends DefinedServiceArtifactFragmentManager<JDBCService> implements CreatableArtifactFragmentManager<JDBCService> {

	private static final String JDBC_SERVICE_PATH = "jdbc-service.xml";
	private static final String QUERY_PATH = "query.sql";
	private static final String PARAMETERS_PATH = "parameters.xml";
	private static final String RESULTS_PATH = "results.xml";
	private static final String ARTIFACT_RESOURCE_PATH = "jdbcservice.xml";
	private static final String XML_CONTENT_TYPE = "application/xml";
	private static final String SQL_CONTENT_TYPE = "application/sql";
	private static final String ARTIFACT_TYPE = "jdbcService";
	private static final String STRUCTURE_FRAGMENT_TYPE = "structure";
	private static final String ARTIFACT_CATEGORY = "service";
	private static final String GUIDELINES_PATH = "/guidelines/jdbc-service.md";
	private static final String CHANGE_TRACKER_INTERFACE = "be.nabu.libs.services.jdbc.api.ChangeTracker.track";

	@Override
	public Entry createArtifact(Entry parent, String name) {
		try {
			RepositoryEntry entry = ((RepositoryEntry) parent).createNode(name, new JDBCServiceManager(), true);
			JDBCService artifact = new JDBCService(entry.getId());
			artifact.setExecutionContextProvider(entry.getRepository());
			artifact.setDataSourceResolver(new RepositoryDataSourceResolver());
			new JDBCServiceManager().save(entry, artifact);
			return entry;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<ArtifactFragment> listFragments(final JDBCService artifact) {
		List<ArtifactFragment> fragments = new ArrayList<ArtifactFragment>(super.listFragments(artifact));
		fragments.add(new ArtifactFragment() {
			@Override
			public boolean isEditable() {
				Entry entry = EAIResourceRepository.getInstance().getEntry(artifact.getId());
				return entry instanceof ResourceEntry && entry.isEditable();
			}

			@Override
			public boolean isRemovable() {
				return false;
			}

			@Override
			public String getPath() {
				return JDBC_SERVICE_PATH;
			}

			@Override
			public String getContent() {
				try {
					return marshalConfigurationFragment(artifact);
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public String getContentType() {
				return XML_CONTENT_TYPE;
			}

			@Override
			public String getArtifactId() {
				return artifact.getId();
			}

			@Override
			public String getFragmentType() {
				return ARTIFACT_TYPE;
			}

			@Override
			public Map<String, String> getProperties() {
				return new LinkedHashMap<String, String>();
			}

			@Override
			public Long getLastModified() {
				return getFragmentLastModified(artifact.getId(), ARTIFACT_RESOURCE_PATH);
			}
		});
		fragments.add(new ArtifactFragment() {
			@Override
			public boolean isEditable() {
				Entry entry = EAIResourceRepository.getInstance().getEntry(artifact.getId());
				return entry instanceof ResourceEntry && entry.isEditable();
			}

			@Override
			public boolean isRemovable() {
				return false;
			}

			@Override
			public String getPath() {
				return QUERY_PATH;
			}

			@Override
			public String getContent() {
				return artifact.getSql() == null ? "" : artifact.getSql();
			}

			@Override
			public String getContentType() {
				return SQL_CONTENT_TYPE;
			}

			@Override
			public String getArtifactId() {
				return artifact.getId();
			}

			@Override
			public String getFragmentType() {
				return ARTIFACT_TYPE;
			}

			@Override
			public Map<String, String> getProperties() {
				return new LinkedHashMap<String, String>();
			}

			@Override
			public Long getLastModified() {
				return getFragmentLastModified(artifact.getId(), ARTIFACT_RESOURCE_PATH);
			}
		});
		if (artifact.isInputGenerated()) {
			fragments.add(new GeneratedStructureFragment(artifact, PARAMETERS_PATH, artifact.getParameters()));
		}
		if (artifact.isOutputGenerated()) {
			fragments.add(new GeneratedStructureFragment(artifact, RESULTS_PATH, artifact.getResults()));
		}
		return fragments;
	}

	@Override
	public List<Validation<?>> updateFragment(JDBCService artifact, String path, String oldContent, String newContent) {
		if (!JDBC_SERVICE_PATH.equals(path) && !QUERY_PATH.equals(path) && !PARAMETERS_PATH.equals(path) && !RESULTS_PATH.equals(path)) {
			return super.updateFragment(artifact, path, oldContent, newContent);
		}
		ResourceEntry entry = (ResourceEntry) EAIResourceRepository.getInstance().getEntry(artifact.getId());
		List<Validation<?>> validations = new ArrayList<Validation<?>>();
		try {
			JDBCService candidate = copyArtifact(entry, artifact);
			if (QUERY_PATH.equals(path)) {
				validations.addAll(candidate.setSql(newContent));
			}
			else if (JDBC_SERVICE_PATH.equals(path)) {
				applyConfigurationFragment(candidate, newContent, validations);
			}
			else {
				applyGeneratedStructureFragment(entry, candidate, path, newContent, validations);
			}
			if (!hasErrors(validations)) {
				validations.addAll(new JDBCServiceManager().save(entry, candidate));
			}
		}
		catch (Exception e) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
		}
		return validations;
	}

	private void applyGeneratedStructureFragment(ResourceEntry entry, JDBCService artifact, String path, String content, List<Validation<?>> validations) throws Exception {
		if (PARAMETERS_PATH.equals(path) && !artifact.isInputGenerated()) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "The parameters.xml fragment can only be updated when the input is generated"));
			return;
		}
		if (RESULTS_PATH.equals(path) && !artifact.isOutputGenerated()) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "The results.xml fragment can only be updated when the output is generated"));
			return;
		}
		ComplexType currentType = PARAMETERS_PATH.equals(path) ? artifact.getParameters() : artifact.getResults();
		if (!(currentType instanceof Structure)) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "Generated JDBC structures must be editable structures"));
			return;
		}
		Set<String> currentFields = getFieldNames(currentType);
		Structure updated = StructureManager.parseUpdatedStructure(entry, content, (Structure) currentType, new DefinedStructure(), validations);
		if (currentType instanceof DefinedType && updated instanceof DefinedStructure) {
			((DefinedStructure) updated).setId(((DefinedType) currentType).getId());
		}
		updated.setName(currentType.getName(currentType.getProperties()));
		if (!currentFields.equals(getFieldNames(updated))) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "Generated JDBC structure fragments may only adapt existing fields; adding or removing fields is not allowed"));
			return;
		}
		if (PARAMETERS_PATH.equals(path)) {
			artifact.setParameters(updated);
			artifact.setInputGenerated(true);
		}
		else {
			artifact.setResults(updated);
			artifact.setOutputGenerated(true);
		}
	}

	private Set<String> getFieldNames(ComplexType type) {
		Set<String> fields = new LinkedHashSet<String>();
		for (be.nabu.libs.types.api.Element<?> element : type) {
			if (element != null) {
				fields.add(element.getName());
			}
		}
		return fields;
	}

	private class GeneratedStructureFragment implements ArtifactFragment {

		private JDBCService artifact;
		private String path;
		private ComplexType type;

		public GeneratedStructureFragment(JDBCService artifact, String path, ComplexType type) {
			this.artifact = artifact;
			this.path = path;
			this.type = type;
		}

		@Override
		public boolean isEditable() {
			Entry entry = EAIResourceRepository.getInstance().getEntry(artifact.getId());
			return entry instanceof ResourceEntry && entry.isEditable();
		}

		@Override
		public boolean isRemovable() {
			return false;
		}

		@Override
		public String getPath() {
			return path;
		}

		@Override
		public String getContent() {
			XMLDefinitionMarshaller marshaller = new XMLDefinitionMarshaller();
			marshaller.setIgnoreUnknownSuperTypes(true);
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			try {
				marshaller.marshal(output, type);
				return new String(output.toByteArray(), StandardCharsets.UTF_8);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String getContentType() {
			return XML_CONTENT_TYPE;
		}

		@Override
		public String getArtifactId() {
			return artifact.getId();
		}

		@Override
		public String getFragmentType() {
			return STRUCTURE_FRAGMENT_TYPE;
		}

		@Override
		public Map<String, String> getProperties() {
			return new LinkedHashMap<String, String>();
		}

		@Override
		public Long getLastModified() {
			return getFragmentLastModified(artifact.getId(), path);
		}
	}

	@Override
	public Class<JDBCService> getArtifactClass() {
		return JDBCService.class;
	}

	@Override
	public String getArtifactType() {
		return ARTIFACT_TYPE;
	}

	@Override
	public String getArtifactCategory() {
		return ARTIFACT_CATEGORY;
	}

	@Override
	public String getGuidelines(List<String> fragmentTypes) {
		List<String> sections = new ArrayList<String>();
		String guidelines = EAIRepositoryUtils.loadCachedClasspathResource(JDBCServiceArtifactFragmentManager.class, GUIDELINES_PATH);
		if (guidelines != null && !guidelines.trim().isEmpty()) {
			sections.add(guidelines.trim());
		}
		String metadataGuidance = super.getGuidelines(fragmentTypes);
		if (metadataGuidance != null && !metadataGuidance.trim().isEmpty()) {
			sections.add(metadataGuidance.trim());
		}
		return String.join("\n\n", sections).trim();
	}

	private String marshalConfigurationFragment(JDBCService artifact) throws Exception {
		JDBCServiceManager.JDBCServiceConfig config = new JDBCServiceManager.JDBCServiceConfig();
		config.setConnectionId(artifact.getConnectionId());
		if (!artifact.isInputGenerated()) {
			config.setInputDefinition(((DefinedType) artifact.getParameters()).getId());
		}
		if (!artifact.isOutputGenerated()) {
			config.setOutputDefinition(((DefinedType) artifact.getResults()).getId());
		}
		config.setValidateInput(artifact.getValidateInput());
		config.setValidateOutput(artifact.getValidateOutput());
		config.setGeneratedColumn(artifact.getGeneratedColumn());
		config.setChangeTrackerId(JDBCServiceManager.getChangeTrackerId(artifact));
		XMLBinding binding = new XMLBinding((ComplexType) BeanResolver.getInstance().resolve(JDBCServiceManager.JDBCServiceConfig.class), StandardCharsets.UTF_8);
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		binding.marshal(output, new BeanInstance<JDBCServiceManager.JDBCServiceConfig>(config));
		Document document = parseDocument(new String(output.toByteArray(), StandardCharsets.UTF_8));
		removeDirectChild(document.getDocumentElement(), "sql");
		return toXml(document);
	}

	private void applyConfigurationFragment(JDBCService artifact, String content, List<Validation<?>> validations) throws Exception {
		Document document = parseDocument(content);
		Element root = document.getDocumentElement();
		if (root == null || !"jdbcService".equals(root.getLocalName() == null ? root.getNodeName() : root.getLocalName())) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "The jdbc-service fragment must contain a jdbcService root element"));
			return;
		}
		if (getDirectChild(root, "sql") != null) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "The jdbc-service.xml fragment must not contain a sql element; use query.sql instead"));
			return;
		}
		Element sql = document.createElement("sql");
		String currentSql = artifact.getSql();
		sql.setTextContent(currentSql == null ? "" : currentSql);
		root.appendChild(sql);
		XMLBinding binding = new XMLBinding((ComplexType) BeanResolver.getInstance().resolve(JDBCServiceManager.JDBCServiceConfig.class), StandardCharsets.UTF_8);
		JDBCServiceManager.JDBCServiceConfig config = be.nabu.libs.types.TypeUtils.getAsBean(binding.unmarshal(new ByteArrayInputStream(contentWithSql(document).getBytes(StandardCharsets.UTF_8)), new Window[0]), JDBCServiceManager.JDBCServiceConfig.class);
		artifact.setConnectionId(config.getConnectionId());
		artifact.setGeneratedColumn(config.getGeneratedColumn());
		artifact.setValidateInput(config.getValidateInput());
		artifact.setValidateOutput(config.getValidateOutput());
		validateChangeTracker(config.getChangeTrackerId(), validations);
		if (!hasErrors(validations)) {
			artifact.setChangeTracker(JDBCServiceManager.getAsChangeTracker(entryRepository(artifact), config.getChangeTrackerId()));
		}
		if (config.getInputDefinition() != null) {
			Object resolved = entryRepository(artifact).resolve(config.getInputDefinition());
			if (!(resolved instanceof ComplexType)) {
				validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "Could not find referenced input definition: " + config.getInputDefinition()));
			}
			else {
				artifact.setParameters((ComplexType) resolved);
				artifact.setInputGenerated(false);
			}
		}
		else {
			artifact.setInputGenerated(true);
		}
		if (config.getOutputDefinition() != null) {
			Object resolved = entryRepository(artifact).resolve(config.getOutputDefinition());
			if (!(resolved instanceof ComplexType)) {
				validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "Could not find referenced output definition: " + config.getOutputDefinition()));
			}
			else {
				artifact.setResults((ComplexType) resolved);
				artifact.setOutputGenerated(false);
			}
		}
		else {
			artifact.setOutputGenerated(true);
		}
	}

	private JDBCService copyArtifact(ResourceEntry entry, JDBCService artifact) {
		JDBCService copy = new JDBCService(artifact.getId());
		copy.setExecutionContextProvider(entry.getRepository());
		copy.setDataSourceResolver(new RepositoryDataSourceResolver());
		copy.setConnectionId(artifact.getConnectionId());
		copy.setSql(artifact.getSql());
		copy.setGeneratedColumn(artifact.getGeneratedColumn());
		copy.setValidateInput(artifact.getValidateInput());
		copy.setValidateOutput(artifact.getValidateOutput());
		copy.setChangeTracker(artifact.getChangeTracker());
		if (artifact.isInputGenerated()) {
			copy.setParameters(artifact.getParameters());
			copy.setInputGenerated(true);
		}
		else {
			copy.setParameters(artifact.getParameters());
			copy.setInputGenerated(false);
		}
		if (artifact.isOutputGenerated()) {
			copy.setResults(artifact.getResults());
			copy.setOutputGenerated(true);
		}
		else {
			copy.setResults(artifact.getResults());
			copy.setOutputGenerated(false);
		}
		return copy;
	}

	private be.nabu.eai.repository.api.Repository entryRepository(JDBCService artifact) {
		return ((ResourceEntry) EAIResourceRepository.getInstance().getEntry(artifact.getId())).getRepository();
	}

	private String contentWithSql(Document document) throws Exception {
		return toXml(document);
	}

	private Document parseDocument(String content) throws Exception {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		return factory.newDocumentBuilder().parse(new java.io.ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
	}

	private String toXml(Document document) throws Exception {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		EAIRepositoryUtils.prettyPrint(document, output);
		return new String(output.toByteArray(), StandardCharsets.UTF_8);
	}

	private void removeDirectChild(Element parent, String name) {
		NodeList children = parent.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			if (child instanceof Element) {
				Element element = (Element) child;
				String childName = element.getLocalName() == null ? element.getNodeName() : element.getLocalName();
				if (name.equals(childName)) {
					parent.removeChild(child);
					return;
				}
			}
		}
	}

	private Element getDirectChild(Element parent, String name) {
		NodeList children = parent.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			if (child instanceof Element) {
				Element element = (Element) child;
				String childName = element.getLocalName() == null ? element.getNodeName() : element.getLocalName();
				if (name.equals(childName)) {
					return element;
				}
			}
		}
		return null;
	}

	private void validateChangeTracker(String changeTrackerId, List<Validation<?>> validations) {
		if (changeTrackerId == null || changeTrackerId.trim().isEmpty()) {
			return;
		}
		Object resolved = EAIResourceRepository.getInstance().resolve(changeTrackerId);
		if (!(resolved instanceof DefinedService)) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "Configured changeTrackerId '" + changeTrackerId + "' is not a defined service"));
			return;
		}
		DefinedServiceInterface iface = DefinedServiceInterfaceResolverFactory.getInstance().getResolver().resolve(CHANGE_TRACKER_INTERFACE);
		if (iface == null) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "Unknown interface requested for changeTrackerId: " + CHANGE_TRACKER_INTERFACE));
			return;
		}
		if (!POJOUtils.isImplementation((DefinedService) resolved, iface)) {
			validations.add(new ValidationMessage(ValidationMessage.Severity.ERROR, "Configured changeTrackerId '" + changeTrackerId + "' does not implement " + CHANGE_TRACKER_INTERFACE));
		}
	}

	private boolean hasErrors(List<Validation<?>> validations) {
		if (validations == null) {
			return false;
		}
		for (Validation<?> validation : validations) {
			if (validation != null && validation.getSeverity() == ValidationMessage.Severity.ERROR) {
				return true;
			}
		}
		return false;
	}
}
