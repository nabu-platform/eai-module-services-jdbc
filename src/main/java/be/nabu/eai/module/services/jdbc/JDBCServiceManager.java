package be.nabu.eai.module.services.jdbc;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import be.nabu.eai.module.types.structure.StructureManager;
import be.nabu.eai.repository.EAINode;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.api.ArtifactManager;
import be.nabu.eai.repository.api.ArtifactRepositoryManager;
import be.nabu.eai.repository.api.BrokenReferenceArtifactManager;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.ModifiableEntry;
import be.nabu.eai.repository.api.ModifiableNodeEntry;
import be.nabu.eai.repository.api.ResourceEntry;
import be.nabu.eai.repository.resources.MemoryEntry;
import be.nabu.libs.artifacts.ArtifactResolverFactory;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.resources.ResourceReadableContainer;
import be.nabu.libs.resources.ResourceWritableContainer;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.resources.api.WritableResource;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Type;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.java.BeanResolver;
import be.nabu.libs.types.structure.DefinedStructure;
import be.nabu.libs.validator.api.Validation;
import be.nabu.libs.validator.api.ValidationMessage;
import be.nabu.libs.validator.api.ValidationMessage.Severity;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;

public class JDBCServiceManager implements ArtifactManager<JDBCService>, ArtifactRepositoryManager<JDBCService>, BrokenReferenceArtifactManager<JDBCService> {

	@Override
	public JDBCService load(ResourceEntry entry, List<Validation<?>> messages) throws IOException, ParseException {
		JDBCService service = new JDBCService(entry.getId());
		Resource resource = entry.getContainer().getChild("jdbcservice.xml");
		if (resource == null) {
			return null;
		}
		XMLBinding binding = new XMLBinding((ComplexType) BeanResolver.getInstance().resolve(JDBCServiceConfig.class), Charset.forName("UTF-8"));
		ReadableContainer<ByteBuffer> readable = new ResourceReadableContainer((ReadableResource) resource);
		try {
			ComplexContent content = binding.unmarshal(IOUtils.toInputStream(readable), new Window[0]);
			// the content can be null if it contains nothing but xsi:nil
			JDBCServiceConfig config = content == null ? new JDBCServiceConfig() : TypeUtils.getAsBean(content, JDBCServiceConfig.class);
			service.setSql(config.getSql());
			service.setConnectionId(config.getConnectionId());
			service.setGeneratedColumn(config.getGeneratedColumn());
			service.setValidateInput(config.getValidateInput());
			service.setValidateOutput(config.getValidateOutput());
			if (config.getInputDefinition() != null) {
				Artifact artifact = entry.getRepository().resolve(config.getInputDefinition());
				if (!(artifact instanceof ComplexType)) {
					throw new IllegalArgumentException("Could not find referenced output node: " + config.getInputDefinition() + " or it is not a complex type");
				}
				service.setParameters((ComplexType) artifact);
				service.setInputGenerated(false);
			}
			if (config.getOutputDefinition() != null) {
				Artifact artifact = entry.getRepository().resolve(config.getOutputDefinition());
				if (!(artifact instanceof ComplexType)) {
					throw new IllegalArgumentException("Could not find referenced output node: " + config.getOutputDefinition() + " or it is not a complex type");
				}
				service.setResults((ComplexType) artifact);
				service.setOutputGenerated(false);
			}
		}
		finally {
			readable.close();
		}
		if (service.isInputGenerated()) {
			service.setParameters((DefinedStructure) StructureManager.parse(entry, "parameters.xml"));
		}
		if (service.isOutputGenerated()) {
			service.setResults((DefinedStructure) StructureManager.parse(entry, "results.xml"));
		}
		return service;
	}

	@Override
	public List<Validation<?>> save(ResourceEntry entry, JDBCService artifact) throws IOException {
		JDBCServiceConfig config = new JDBCServiceConfig();
		config.setConnectionId(artifact.getConnectionId());
		config.setSql(artifact.getSql());
		if (artifact.isInputGenerated()) {
			StructureManager.format(entry, artifact.getParameters(), "parameters.xml");
		}
		else {
			config.setInputDefinition(((DefinedType) artifact.getParameters()).getId());
			((ManageableContainer<?>) entry.getContainer()).delete("parameters.xml");
		}
		if (artifact.isOutputGenerated()) {
			StructureManager.format(entry, artifact.getResults(), "results.xml");
		}
		else {
			config.setOutputDefinition(((DefinedType) artifact.getResults()).getId());
			((ManageableContainer<?>) entry.getContainer()).delete("results.xml");
		}
		config.setValidateInput(artifact.getValidateInput());
		config.setValidateOutput(artifact.getValidateOutput());
		config.setGeneratedColumn(artifact.getGeneratedColumn());
		Resource resource = entry.getContainer().getChild("jdbcservice.xml");
		if (resource == null) {
			resource = ((ManageableContainer<?>) entry.getContainer()).create("jdbcservice.xml", "application/xml");
		}
		XMLBinding binding = new XMLBinding((ComplexType) BeanResolver.getInstance().resolve(JDBCServiceConfig.class), Charset.forName("UTF-8"));
		WritableContainer<ByteBuffer> writable = new ResourceWritableContainer((WritableResource) resource);
		try {
			binding.marshal(IOUtils.toOutputStream(writable), new BeanInstance<JDBCServiceConfig>(config));
		}
		finally {
			writable.close();
		}
		if (entry instanceof ModifiableNodeEntry) {
			((ModifiableNodeEntry) entry).updateNode(getReferences(artifact));
		}
		return new ArrayList<Validation<?>>();
	}

	@Override
	public Class<JDBCService> getArtifactClass() {
		return JDBCService.class;
	}
	
	@XmlRootElement(name = "jdbcService")
	public static class JDBCServiceConfig {
		private String connectionId, sql, inputDefinition, outputDefinition, generatedColumn;
		private Boolean validateInput, validateOutput;

		public String getConnectionId() {
			return connectionId;
		}

		public void setConnectionId(String connectionId) {
			this.connectionId = connectionId;
		}

		public String getSql() {
			return sql;
		}

		public void setSql(String sql) {
			this.sql = sql;
		}

		public String getInputDefinition() {
			return inputDefinition;
		}

		public void setInputDefinition(String inputDefinition) {
			this.inputDefinition = inputDefinition;
		}

		public String getOutputDefinition() {
			return outputDefinition;
		}

		public void setOutputDefinition(String outputDefinition) {
			this.outputDefinition = outputDefinition;
		}

		public Boolean getValidateInput() {
			return validateInput;
		}

		public void setValidateInput(Boolean validateInput) {
			this.validateInput = validateInput;
		}

		public Boolean getValidateOutput() {
			return validateOutput;
		}

		public void setValidateOutput(Boolean validateOutput) {
			this.validateOutput = validateOutput;
		}

		public String getGeneratedColumn() {
			return generatedColumn;
		}

		public void setGeneratedColumn(String generatedColumn) {
			this.generatedColumn = generatedColumn;
		}
	}

	@Override
	public List<Entry> addChildren(ModifiableEntry parent, JDBCService artifact) {
		List<Entry> entries = new ArrayList<Entry>();
		if (artifact != null && (artifact.isInputGenerated() || artifact.isOutputGenerated())) {
			boolean isLeaf = true;
			if (artifact.isInputGenerated() && TypeUtils.getAllChildren(artifact.getParameters()).size() > 0) {
				isLeaf = false;
				EAINode node = new EAINode();
				node.setArtifactClass(DefinedStructure.class);
				node.setArtifact((DefinedStructure) artifact.getParameters());
				node.setLeaf(true);
				Entry parameters = new MemoryEntry(parent.getRepository(), parent, node, parent.getId() + "." + JDBCService.PARAMETERS, JDBCService.PARAMETERS);
				// need to explicitly set id (it was loaded from file)
				((DefinedStructure) artifact.getParameters()).setId(parameters.getId());
				node.setEntry(parameters);
				parent.addChildren(parameters);
				entries.add(parameters);
			}
			if (artifact.isOutputGenerated() && TypeUtils.getAllChildren(artifact.getResults()).size() > 0) {
				isLeaf = false;
				EAINode node = new EAINode();
				node.setArtifactClass(DefinedStructure.class);
				node.setArtifact((DefinedStructure) artifact.getResults());
				node.setLeaf(true);
				Entry results = new MemoryEntry(parent.getRepository(), parent, node, parent.getId() + "." + JDBCService.RESULTS, JDBCService.RESULTS);
				((DefinedStructure) artifact.getResults()).setId(results.getId());
				node.setEntry(results);
				parent.addChildren(results);
				entries.add(results);
			}
			((EAINode) parent.getNode()).setLeaf(isLeaf);
		}
		return entries;
	}

	@Override
	public List<Entry> removeChildren(ModifiableEntry parent, JDBCService artifact) {
		List<Entry> entries = new ArrayList<Entry>();
		Entry parameters = parent.getChild(JDBCService.PARAMETERS);
		if (parameters != null) {
			((ModifiableEntry) parent).removeChildren(parameters.getName());
			entries.add(parameters);
		}
		Entry results = parent.getChild(JDBCService.RESULTS);
		if (results != null) {
			((ModifiableEntry) parent).removeChildren(results.getName());
			entries.add(results);
		}
		((EAINode) parent.getNode()).setLeaf(true);
		return entries;
	}
	
	
	public void refreshChildren(ModifiableEntry parent, JDBCService artifact) {
		removeChildren((ModifiableEntry) parent, artifact);
		addChildren((ModifiableEntry) parent, artifact);
	}

	@Override
	public List<String> getReferences(JDBCService artifact) throws IOException {
		List<String> references = new ArrayList<String>();
		if (artifact.getConnectionId() != null) {
			references.add(artifact.getConnectionId());
		}
		references.addAll(StructureManager.getComplexReferences(artifact.getInput()));
		references.addAll(StructureManager.getComplexReferences(artifact.getOutput()));
		// a "null" reference can be injected if an parameters/result document is empty because it does exist in the representation but is not actually added to the repository tree to avoid clutter
		references.remove(null);
		return references;
	}

	@Override
	public List<Validation<?>> updateReference(JDBCService artifact, String from, String to) throws IOException {
		if (from.equals(artifact.getConnectionId())) {
			artifact.setConnectionId(to);
		}
		List<Validation<?>> messages = new ArrayList<Validation<?>>();
		if (artifact.isInputGenerated()) {
			messages.addAll(StructureManager.updateReferences(artifact.getInput(), from, to));
		}
		else if (from.equals(((DefinedType) artifact.getParameters()).getId())) {
			Artifact newType = ArtifactResolverFactory.getInstance().getResolver().resolve(to);
			if (!(newType instanceof Type)) {
				messages.add(new ValidationMessage(Severity.ERROR, "Not a type: " + to));	
			}
			else {
				artifact.setParameters((ComplexType) newType);
			}
		}
		if (artifact.isOutputGenerated()) {
			messages.addAll(StructureManager.updateReferences(artifact.getOutput(), from, to));
		}
		else if (from.equals(((DefinedType) artifact.getResults()).getId())) {
			Artifact newType = ArtifactResolverFactory.getInstance().getResolver().resolve(to);
			if (!(newType instanceof Type)) {
				messages.add(new ValidationMessage(Severity.ERROR, "Not a type: " + to));	
			}
			else {
				artifact.setResults((ComplexType) newType);
			}
		}
		return messages;
	}

	@Override
	public List<Validation<?>> updateBrokenReference(ResourceContainer<?> container, String from, String to) throws IOException {
		List<Validation<?>> messages = new ArrayList<Validation<?>>();
		Resource child = container.getChild("jdbcservice.xml");
		if (child != null) {
			EAIRepositoryUtils.updateBrokenReference(child, from, to, Charset.forName("UTF-8"));
		}
		return messages;
	}
}
