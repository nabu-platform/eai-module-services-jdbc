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

package be.nabu.eai.module.services.jdbc;

import java.io.IOException;
import java.util.List;

import javafx.scene.layout.AnchorPane;
import be.nabu.eai.developer.MainController;
import be.nabu.eai.developer.api.RefresheableArtifactGUIInstance;
import be.nabu.eai.repository.api.ResourceEntry;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.validator.api.Validation;

public class JDBCServiceGUIInstance implements RefresheableArtifactGUIInstance {

	private JDBCService service;
	private ResourceEntry entry;
	private boolean changed;
	private JDBCServiceGUIManager manager;
	
	public JDBCServiceGUIInstance(JDBCServiceGUIManager manager) {
		this.manager = manager;
		// delayed
	}
	
	public JDBCServiceGUIInstance(JDBCServiceGUIManager manager, ResourceEntry entry) {
		this.manager = manager;
		this.entry = entry;
	}

	public JDBCService getService() {
		return service;
	}

	public void setService(JDBCService service) {
		this.service = service;
	}

	@Override
	public String getId() {
		return entry.getId();
	}

	@Override
	public List<Validation<?>> save() throws IOException {
		manager.syncBeforeSave(service);
		return new JDBCServiceManager().save(entry, service);
	}

	@Override
	public boolean hasChanged() {
		return changed;
	}

	@Override
	public boolean isReady() {
		return service != null && entry != null;
	}

	@Override
	public boolean isEditable() {
		return true;
	}

	public ResourceEntry getEntry() {
		return entry;
	}

	public void setEntry(ResourceEntry entry) {
		this.entry = entry;
	}

	@Override
	public void setChanged(boolean changed) {
		this.changed = changed;
	}

	@Override
	public void refresh(AnchorPane pane) {
		entry.refresh(true);
		try {
			this.service = manager.display(MainController.getInstance(), pane, entry);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not refresh: " + getId(), e);
		}
	}

	@Override
	public Artifact getArtifact() {
		return service;
	}
	
}
