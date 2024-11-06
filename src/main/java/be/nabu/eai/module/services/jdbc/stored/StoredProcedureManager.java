/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.eai.module.services.jdbc.stored;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import be.nabu.eai.module.types.structure.StructureManager;
import be.nabu.eai.repository.EAINode;
import be.nabu.eai.repository.api.ArtifactRepositoryManager;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.ModifiableEntry;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.api.ResourceEntry;
import be.nabu.eai.repository.managers.base.JAXBArtifactManager;
import be.nabu.eai.repository.resources.MemoryEntry;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.structure.DefinedStructure;
import be.nabu.libs.validator.api.Validation;

public class StoredProcedureManager extends JAXBArtifactManager<StoredProcedureConfiguration, StoredProcedureArtifact> implements ArtifactRepositoryManager<StoredProcedureArtifact> {

	public StoredProcedureManager() {
		super(StoredProcedureArtifact.class);
	}

	@Override
	public StoredProcedureArtifact load(ResourceEntry entry, List<Validation<?>> messages) throws IOException, ParseException {
		StoredProcedureArtifact artifact = super.load(entry, messages);
		
		if (artifact.hasParameters()) {
			artifact.setParameters((DefinedStructure) StructureManager.parse(entry, "parameters.xml"));
		}
		if (artifact.hasResult()) {
			artifact.setResults((DefinedStructure) StructureManager.parse(entry, "results.xml"));
		}
		if (artifact.hasReturnValue()) {
			artifact.setReturnValue((DefinedStructure) StructureManager.parse(entry, "return.xml"));
		}
		return artifact;
	}

	@Override
	public List<Validation<?>> save(ResourceEntry entry, StoredProcedureArtifact artifact) throws IOException {
		if (artifact.hasParameters()) {
			StructureManager.format(entry, artifact.getParameters(), "parameters.xml");
		}
		else {
			((ManageableContainer<?>) entry.getContainer()).delete("parameters.xml");
		}
		
		if (artifact.hasResult()) {
			StructureManager.format(entry, artifact.getResults(), "results.xml");
		}
		else {
			((ManageableContainer<?>) entry.getContainer()).delete("results.xml");
		}
		
		if (artifact.hasReturnValue()) {
			StructureManager.format(entry, artifact.getReturnValue(), "return.xml");
		}
		else {
			((ManageableContainer<?>) entry.getContainer()).delete("return.xml");
		}
		return super.save(entry, artifact);
	}

	@Override
	protected StoredProcedureArtifact newInstance(String id, ResourceContainer<?> container, Repository repository) {
		return new StoredProcedureArtifact(id, container, repository);
	}

	@Override
	public List<Entry> addChildren(ModifiableEntry parent, StoredProcedureArtifact artifact) {
		List<Entry> entries = new ArrayList<Entry>();
		if (artifact != null) {
			boolean isLeaf = true;
			if (artifact.hasResult() && TypeUtils.getAllChildren(artifact.getResults()).size() > 0) {
				isLeaf = false;
				EAINode node = new EAINode();
				node.setArtifactClass(DefinedStructure.class);
				node.setArtifact((DefinedStructure) artifact.getResults());
				node.setLeaf(true);
				Entry results = new MemoryEntry(artifact.getId(), parent.getRepository(), parent, node, parent.getId() + "." + JDBCService.RESULTS, JDBCService.RESULTS);
				((DefinedStructure) artifact.getResults()).setId(results.getId());
				node.setEntry(results);
				parent.addChildren(results);
				entries.add(results);
			}
			if (artifact.hasParameters() && TypeUtils.getAllChildren(artifact.getParameters()).size() > 0) {
				isLeaf = false;
				EAINode node = new EAINode();
				node.setArtifactClass(DefinedStructure.class);
				node.setArtifact((DefinedStructure) artifact.getParameters());
				node.setLeaf(true);
				Entry results = new MemoryEntry(artifact.getId(), parent.getRepository(), parent, node, parent.getId() + "." + JDBCService.PARAMETERS, JDBCService.PARAMETERS);
				((DefinedStructure) artifact.getParameters()).setId(results.getId());
				node.setEntry(results);
				parent.addChildren(results);
				entries.add(results);
			}
			if (artifact.hasReturnValue() && TypeUtils.getAllChildren(artifact.getReturnValue()).size() > 0) {
				isLeaf = false;
				EAINode node = new EAINode();
				node.setArtifactClass(DefinedStructure.class);
				node.setArtifact((DefinedStructure) artifact.getReturnValue());
				node.setLeaf(true);
				Entry results = new MemoryEntry(artifact.getId(), parent.getRepository(), parent, node, parent.getId() + ".return", "return");
				((DefinedStructure) artifact.getReturnValue()).setId(results.getId());
				node.setEntry(results);
				parent.addChildren(results);
				entries.add(results);
			}
			((EAINode) parent.getNode()).setLeaf(isLeaf);
		}
		return entries;
	}

	@Override
	public List<Entry> removeChildren(ModifiableEntry parent, StoredProcedureArtifact artifact) {
		List<Entry> entries = new ArrayList<Entry>();
		Entry results = parent.getChild(JDBCService.RESULTS);
		if (results != null) {
			((ModifiableEntry) parent).removeChildren(results.getName());
			entries.add(results);
		}
		results = parent.getChild(JDBCService.PARAMETERS);
		if (results != null) {
			((ModifiableEntry) parent).removeChildren(results.getName());
			entries.add(results);
		}
		results = parent.getChild("return");
		if (results != null) {
			((ModifiableEntry) parent).removeChildren(results.getName());
			entries.add(results);
		}
		((EAINode) parent.getNode()).setLeaf(true);
		return entries;
	}
	
	public void refreshChildren(ModifiableEntry parent, StoredProcedureArtifact artifact) {
		removeChildren((ModifiableEntry) parent, artifact);
		addChildren((ModifiableEntry) parent, artifact);
	}
	
}
