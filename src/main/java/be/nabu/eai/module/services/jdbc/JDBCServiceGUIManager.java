package be.nabu.eai.module.services.jdbc;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.ActionEvent;
import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import be.nabu.eai.developer.MainController;
import be.nabu.eai.developer.api.ArtifactGUIInstance;
import be.nabu.eai.developer.api.ArtifactGUIManager;
import be.nabu.eai.developer.managers.ServiceGUIManager;
import be.nabu.eai.developer.managers.util.ElementMarshallable;
import be.nabu.eai.developer.managers.util.SimpleProperty;
import be.nabu.eai.developer.managers.util.SimplePropertyUpdater;
import be.nabu.eai.developer.util.EAIDeveloperUtils;
import be.nabu.eai.developer.util.ElementSelectionListener;
import be.nabu.eai.developer.util.ElementTreeItem;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.ModifiableEntry;
import be.nabu.eai.repository.api.Node;
import be.nabu.eai.repository.api.ResourceEntry;
import be.nabu.eai.repository.resources.RepositoryEntry;
import be.nabu.jfx.control.ace.AceEditor;
import be.nabu.jfx.control.tree.Tree;
import be.nabu.jfx.control.tree.TreeItem;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Property;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.services.jdbc.JDBCUtils;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.base.RootElement;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.GeneratedProperty;
import be.nabu.libs.types.properties.HiddenProperty;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.libs.types.properties.TimezoneProperty;
import be.nabu.libs.types.properties.TranslatableProperty;
import be.nabu.libs.validator.api.ValidationMessage;
import be.nabu.libs.validator.api.ValidationMessage.Severity;

public class JDBCServiceGUIManager implements ArtifactGUIManager<JDBCService> {

	private TextArea area;
	private AceEditor editor;

	@Override
	public JDBCServiceManager getArtifactManager() {
		return new JDBCServiceManager();
	}

	@Override
	public String getArtifactName() {
		return "JDBC Service";
	}

	@Override
	public ImageView getGraphic() {
		return MainController.loadGraphic("jdbcservice.png");
	}

	@Override
	public Class<JDBCService> getArtifactClass() {
		return JDBCService.class;
	}
	
	@Override
	public String getCategory() {
		return "Services";
	}

	@Override
	public ArtifactGUIInstance create(final MainController controller, final TreeItem<Entry> target) throws IOException {
		List<Property<?>> properties = new ArrayList<Property<?>>();
		properties.add(new SimpleProperty<String>("Name", String.class, true));
		final SimplePropertyUpdater updater = new SimplePropertyUpdater(true, new LinkedHashSet<Property<?>>(properties));
		final JDBCServiceGUIInstance instance = new JDBCServiceGUIInstance(this);
		EAIDeveloperUtils.buildPopup(controller, updater, "Create JDBC Service", new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent arg0) {
				try {
					String name = updater.getValue("Name");
					RepositoryEntry entry = ((RepositoryEntry) target.itemProperty().get()).createNode(name, getArtifactManager(), true);
					JDBCService service = new JDBCService(entry.getId());
					service.setDataSourceResolver(new RepositoryDataSourceResolver());
					getArtifactManager().save(entry, service);
					entry.getRepository().reload(target.itemProperty().get().getId());
					EAIDeveloperUtils.reload(entry.getId(), true);
					//controller.getRepositoryBrowser().refresh();
					
					// reload
					MainController.getInstance().getAsynchronousRemoteServer().reload(target.itemProperty().get().getId());
					MainController.getInstance().getCollaborationClient().created(entry.getId(), "Created");
					
					Tab tab = controller.newTab(entry.getId(), instance);
					AnchorPane pane = new AnchorPane();
					tab.setContent(pane);
					instance.setEntry(entry);
					instance.setService(display(controller, pane, entry));
					ServiceGUIManager.makeRunnable(tab, service, controller);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
				catch (ParseException e) {
					throw new RuntimeException(e);
				}
			}
		});
		return instance;
	}

	@Override
	public ArtifactGUIInstance view(MainController controller, TreeItem<Entry> target) throws IOException, ParseException {
		JDBCServiceGUIInstance instance = new JDBCServiceGUIInstance(this, (ResourceEntry) target.itemProperty().get());
		Tab tab = controller.newTab(target.itemProperty().get().getId(), instance);
		AnchorPane pane = new AnchorPane();
		tab.setContent(pane);
		instance.setService(display(controller, pane, target.itemProperty().get()));
		ServiceGUIManager.makeRunnable(tab, instance.getService(), controller);
		return instance;
	}

	JDBCService display(final MainController controller, Pane pane, final Entry entry) throws IOException, ParseException {
		final JDBCService service = (JDBCService) entry.getNode().getArtifact();
		SplitPane main = new SplitPane();
		
		AnchorPane top = new AnchorPane();
		main.setOrientation(Orientation.VERTICAL);
		SplitPane iface = new SplitPane();
		iface.setOrientation(Orientation.HORIZONTAL);
		main.getItems().addAll(top, iface);

		ElementSelectionListener elementSelectionListener = new ElementSelectionListener(controller, false, true, FormatProperty.getInstance(), TimezoneProperty.getInstance(), 
			MinOccursProperty.getInstance(), MaxOccursProperty.getInstance(), PrimaryKeyProperty.getInstance(), CollectionNameProperty.getInstance(), TranslatableProperty.getInstance());
		elementSelectionListener.setForceAllowUpdate(true);
		elementSelectionListener.setActualId(entry.getId());
		
		ScrollPane left = new ScrollPane();
		VBox leftBox = new VBox();
		final Tree<Element<?>> input = new Tree<Element<?>>(new ElementMarshallable());
		if (service.isInputGenerated()) {
			// TODO: introduce locking
			ElementTreeItem.setListeners(input, new SimpleBooleanProperty(true), true);
		}
		EAIDeveloperUtils.addElementExpansionHandler(input);
		input.rootProperty().set(new ElementTreeItem(new RootElement(service.getInput()), null, false, false));
		left.setContent(leftBox);
		input.prefWidthProperty().bind(left.widthProperty());
		input.getSelectionModel().selectedItemProperty().addListener(elementSelectionListener);
		
		CheckBox validateInput = new CheckBox("Validate Input");
		validateInput.setPadding(new Insets(10, 20, 0, 0));
//		validateInput.setPadding(new Insets(14, 20, 10, 15));
//		validateInput.setTooltip(new Tooltip("Validate Input"));
		validateInput.setSelected(service.getValidateInput() != null && service.getValidateInput());
		validateInput.selectedProperty().addListener(new ChangeListener<Boolean>() {
			@Override
			public void changed(ObservableValue<? extends Boolean> arg0, Boolean arg1, Boolean arg2) {
				service.setValidateInput(arg2);
				MainController.getInstance().setChanged();
			}
		});
		CheckBox validateOutput = new CheckBox("Validate Output");
		validateOutput.setPadding(new Insets(10, 20, 0, 0));
//		validateOutput.setPadding(new Insets(14, 20, 10, 15));
//		validateOutput.setTooltip(new Tooltip("Validate Output"));
		validateOutput.setSelected(service.getValidateOutput() != null && service.getValidateOutput());
		validateOutput.selectedProperty().addListener(new ChangeListener<Boolean>() {
			@Override
			public void changed(ObservableValue<? extends Boolean> arg0, Boolean arg1, Boolean arg2) {
				service.setValidateOutput(arg2);
				MainController.getInstance().setChanged();
			}
		});
		
		HBox namedInput = new HBox();
		namedInput.setPadding(new Insets(10));
		namedInput.getChildren().addAll(validateInput);
//		new Label("Input definition: ")
		TextField inputField = new TextField();
		HBox.setHgrow(inputField, Priority.ALWAYS);
		inputField.setPromptText("Input Definition");
		final Button generateInsert = new Button("Insert");
		generateInsert.setGraphic(MainController.loadGraphic("edit-button.png"));
		generateInsert.disableProperty().set(service.isInputGenerated());
		final Button generateUpdate = new Button("Update");
		generateUpdate.setGraphic(MainController.loadGraphic("edit-button.png"));
		generateUpdate.disableProperty().set(service.isInputGenerated());
		if (!service.isInputGenerated()) {
			inputField.textProperty().set(((DefinedType) service.getParameters()).getId());
		}
		inputField.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> arg0, String arg1, String arg2) {
				generateUpdate.disableProperty().set(true);
				generateInsert.disableProperty().set(true);
				if (arg2 == null || arg2.isEmpty()) {
					if (!service.isInputGenerated()) {
						service.setInputGenerated(true);
						// wipe the input so it can be rebuilt
						service.setParameters(null);
						getArtifactManager().refreshChildren((ModifiableEntry) entry, service);
						EAIDeveloperUtils.reload(entry.getId(), true);
						//controller.getTree().refresh();
						MainController.getInstance().setChanged();
					}
				}
				else {
					Artifact artifact = controller.getRepository().resolve(arg2);
					if (artifact != null && artifact instanceof ComplexType) {
						service.setParameters((ComplexType) artifact);
						service.setInputGenerated(false);
						generateInsert.disableProperty().set(false);
						generateUpdate.disableProperty().set(false);
						getArtifactManager().refreshChildren((ModifiableEntry) entry, service);
						EAIDeveloperUtils.reload(entry.getId(), true);
//						controller.getTree().refresh();
						MainController.getInstance().setChanged();
					}
					else {
						controller.notify(new ValidationMessage(Severity.ERROR, "The indicated node is not a complex type: " + arg2));
					}
				}
				input.refresh();
			}
		});
		namedInput.getChildren().addAll(inputField, generateInsert, generateUpdate);
		leftBox.getChildren().addAll(namedInput, input);
		
		ScrollPane right = new ScrollPane();
		VBox rightBox = new VBox();
		final Tree<Element<?>> output = new Tree<Element<?>>(new ElementMarshallable());
		if (service.isOutputGenerated()) {
			// TODO: introduce locking
			ElementTreeItem.setListeners(input, new SimpleBooleanProperty(true), true);
		}
		EAIDeveloperUtils.addElementExpansionHandler(output);
		output.rootProperty().set(new ElementTreeItem(new RootElement(service.getOutput()), null, false, false));
		right.setContent(rightBox);
		output.prefWidthProperty().bind(right.widthProperty());
		output.getSelectionModel().selectedItemProperty().addListener(elementSelectionListener);
		HBox namedOutput = new HBox();
		namedOutput.setPadding(new Insets(10));
		namedOutput.getChildren().addAll(validateOutput);
//		new Label("Output definition: ")
		final Button generateSelect = new Button("Select");
		generateSelect.setGraphic(MainController.loadGraphic("edit-button.png"));
		generateSelect.disableProperty().set(service.isOutputGenerated());
		TextField outputField = new TextField();
		HBox.setHgrow(outputField, Priority.ALWAYS);
		outputField.setPromptText("Output definition");
		if (!service.isOutputGenerated()) {
			outputField.textProperty().set(((DefinedType) service.getResults()).getId());
		}
		outputField.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> arg0, String arg1, String arg2) {
				generateSelect.disableProperty().set(true);
				if (arg2 == null || arg2.isEmpty()) {
					if (!service.isOutputGenerated()) {
						service.setOutputGenerated(true);
						// wipe the output so it can be rebuilt
						service.setResults(null);
						getArtifactManager().refreshChildren((ModifiableEntry) entry, service);
//						controller.getTree().refresh();
						EAIDeveloperUtils.reload(entry.getId(), true);
						MainController.getInstance().setChanged();
					}
				}
				else {
					Node node = controller.getRepository().getNode(arg2);
					try {
						if (node != null && node.getArtifact() instanceof ComplexType) {
							service.setResults((ComplexType) node.getArtifact());
							service.setOutputGenerated(false);
							generateSelect.disableProperty().set(false);
							getArtifactManager().refreshChildren((ModifiableEntry) entry, service);
//							controller.getTree().refresh();
							EAIDeveloperUtils.reload(entry.getId(), true);
							MainController.getInstance().setChanged();
						}
						else {
							controller.notify(new ValidationMessage(Severity.ERROR, "The indicated node is not a complex type: " + arg2));
						}
					}
					catch (IOException e) {
						e.printStackTrace();
						controller.notify(new ValidationMessage(Severity.ERROR, "Can not parse " + arg2));
					}
					catch (ParseException e) {
						e.printStackTrace();
						controller.notify(new ValidationMessage(Severity.ERROR, "Can not parse " + arg2));
					}
				}
				output.refresh();
			}
		});
		namedOutput.getChildren().addAll(outputField, generateSelect);
		rightBox.getChildren().addAll(namedOutput, output);
		
		iface.getItems().addAll(left, right);
		
		VBox vbox = new VBox();
		
		HBox hbox = new HBox();
		TextField field = new TextField(service.getConnectionId());
		field.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> arg0, String arg1, String arg2) {
				if (arg2 != null && !arg2.trim().isEmpty()) {
					try {
						if (entry.getRepository().getNode(arg2) != null && entry.getRepository().getNode(arg2).getArtifact() instanceof DataSourceWithDialectProviderArtifact) {
							service.setConnectionId(arg2);
							MainController.getInstance().setChanged();
						}
					}
					catch (IOException e) {
						throw new RuntimeException(e);
					}
					catch (ParseException e) {
						throw new RuntimeException(e);
					}
				}
				else {
					service.setConnectionId(null);
					MainController.getInstance().setChanged();
				}
			}
		});
		Label label = new Label("Connection Id: ");
		label.setPadding(new Insets(4, 10, 0, 5));
		hbox.getChildren().addAll(label, field);

		HBox changeTrackerBox = new HBox();
		TextField changeTrackerField = new TextField(JDBCServiceManager.getChangeTrackerId(service));
		changeTrackerField.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> arg0, String arg1, String arg2) {
				if (arg2 != null && !arg2.trim().isEmpty()) {
					try {
						if (entry.getRepository().getNode(arg2) != null && entry.getRepository().getNode(arg2).getArtifact() instanceof Service) {
							service.setChangeTracker(JDBCServiceManager.getAsChangeTracker(entry.getRepository(), arg2));
							MainController.getInstance().setChanged();
						}
					}
					catch (IOException e) {
						throw new RuntimeException(e);
					}
					catch (ParseException e) {
						throw new RuntimeException(e);
					}
				}
				else {
					service.setChangeTracker(null);
					MainController.getInstance().setChanged();
				}
			}
		});
		Label label2 = new Label("Change Tracker Service: ");
		label2.setPadding(new Insets(4, 10, 0, 5));
		changeTrackerBox.getChildren().addAll(label2, changeTrackerField);
		
		HBox generatedColumnBox = new HBox();
		TextField generatedColumn = new TextField(service.getGeneratedColumn());
		generatedColumn.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> arg0, String arg1, String arg2) {
				service.setGeneratedColumn(arg2 == null || arg2.trim().isEmpty() ? null : arg2);
				MainController.getInstance().setChanged();
			}
		});
		Label label3 = new Label("Generated Column Name: ");
		label3.setPadding(new Insets(4, 10, 0, 5));
		generatedColumnBox.getChildren().addAll(label3, generatedColumn);
		
		label.setPrefWidth(200);
		label.setAlignment(Pos.CENTER_RIGHT);
		
		label2.setPrefWidth(200);
		label2.setAlignment(Pos.CENTER_RIGHT);
		
		label3.setPrefWidth(200);
		label3.setAlignment(Pos.CENTER_RIGHT);
		
		hbox.setPadding(new Insets(15, 5, 3, 5));
		generatedColumnBox.setPadding(new Insets(3, 5, 3, 5));
		changeTrackerBox.setPadding(new Insets(3, 5, 15, 5));
		
		HBox.setHgrow(field, Priority.ALWAYS);
		HBox.setHgrow(changeTrackerField, Priority.ALWAYS);
		HBox.setHgrow(generatedColumn, Priority.ALWAYS);
		
		editor = new AceEditor();
		VBox.setVgrow(editor.getWebView(), Priority.ALWAYS);
		editor.getWebView().focusedProperty().addListener(new ChangeListener<Boolean>() {
			@Override
			public void changed(ObservableValue<? extends Boolean> arg0, Boolean arg1, Boolean arg2) {
				if (!arg2) {
					controller.notify(service.setSql(editor.getContent()).toArray(new ValidationMessage[0]));
					getArtifactManager().refreshChildren((ModifiableEntry) entry, service);
//					controller.getTree().refresh();
					EAIDeveloperUtils.reload(entry.getId(), true);
					input.getTreeCell(input.rootProperty().get()).refresh();
					output.getTreeCell(output.rootProperty().get()).refresh();
				}
			}
		});
		editor.setContent("text/sql", service.getSql() == null ? "" : service.getSql());
		editor.subscribe(AceEditor.CHANGE, new EventHandler<Event>() {
			@Override
			public void handle(Event arg0) {
				service.setSql(editor.getContent());
				MainController.getInstance().setChanged();
			}
		});
		editor.subscribe(AceEditor.SAVE, new EventHandler<Event>() {
			@Override
			public void handle(Event arg0) {
				try {
					MainController.getInstance().save(service.getId());
				}
				catch (IOException e) {
					MainController.getInstance().notify(e);
				}
			}
		});
		editor.subscribe(AceEditor.CLOSE, new EventHandler<Event>() {
			@Override
			public void handle(Event arg0) {
				MainController.getInstance().close(service.getId());
			}
		});
		
		area = new TextArea();
		VBox.setVgrow(area, Priority.ALWAYS);
		area.focusedProperty().addListener(new ChangeListener<Boolean>() {
			@Override
			public void changed(ObservableValue<? extends Boolean> arg0, Boolean arg1, Boolean arg2) {
				if (!arg2) {
					controller.notify(service.setSql(area.getText()).toArray(new ValidationMessage[0]));
					getArtifactManager().refreshChildren((ModifiableEntry) entry, service);
//					controller.getTree().refresh();
					EAIDeveloperUtils.reload(entry.getId(), true);
					input.getTreeCell(input.rootProperty().get()).refresh();
					output.getTreeCell(output.rootProperty().get()).refresh();
				}
			}
		});
		area.setText(service.getSql());
		area.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> arg0, String arg1, String arg2) {
				MainController.getInstance().setChanged();
			}
		});
		vbox.getChildren().addAll(hbox, generatedColumnBox, changeTrackerBox, editor.getWebView());
		
		top.getChildren().add(vbox);
		AnchorPane.setBottomAnchor(vbox, 0d);
		AnchorPane.setTopAnchor(vbox, 0d);
		AnchorPane.setLeftAnchor(vbox, 0d);
		AnchorPane.setRightAnchor(vbox, 0d);
		
		pane.getChildren().add(main);
		
		AnchorPane.setBottomAnchor(main, 0d);
		AnchorPane.setTopAnchor(main, 0d);
		AnchorPane.setLeftAnchor(main, 0d);
		AnchorPane.setRightAnchor(main, 0d);
		
		generateInsert(generateInsert, service, area, editor);
		generateUpdate(generateUpdate, service, area, editor);
		generateSelect(generateSelect, service, area, editor);
		
		return service;
	}
	
	private void generateInsert(Button button, final JDBCService service, final TextArea target, final AceEditor editor) {
		button.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent event) {
				StringBuilder sql = new StringBuilder();
				String idField = null;
				for (Element<?> child : TypeUtils.getAllChildren(service.getParameters())) {
					// if it is generated, we don't put it in the database by default
					Value<Boolean> property = child.getProperty(GeneratedProperty.getInstance());
					if (property != null && property.getValue() != null && property.getValue()) {
						continue;
					}
					if (child.getName().equalsIgnoreCase("id")) {
						idField = child.getName();
					}
					if (!sql.toString().isEmpty()) {
						sql.append(",\n");
					}
					sql.append("\t" + child.getName());
				}
				String insertSql = "insert into ~" + EAIRepositoryUtils.uncamelify(JDBCUtils.getTypeName(service.getParameters(), true)) + " (\n" + EAIRepositoryUtils.uncamelify(sql.toString()) + "\n) values (\n" + sql.toString().replaceAll("([\\w]+)", ":$1") + "\n)";
				if (button.getText().contains("Merge")) {
					insertSql += "\non conflict(" + idField + ") do update set";
					insertSql += "\n" + EAIRepositoryUtils.uncamelify(sql.toString()).replaceAll("([\\w]+)", "$1 = excluded.$1");
					button.setText("Insert");
					button.setGraphic(MainController.loadGraphic("edit-button.png"));
				}
				else {
					button.setText("Merge Insert");
					button.setGraphic(MainController.loadGraphic("edit-button.png"));
				}
//				target.textProperty().set(insertSql);
				editor.setContent("text/sql", insertSql);
				MainController.getInstance().setChanged();
			}
		});
	}
	
	private void generateUpdate(Button button, final JDBCService service, final TextArea target, final AceEditor editor) {
		button.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent event) {
				if (button.getText().contains("Merge")) {
					StringBuilder sql = new StringBuilder();
					String idField = null;
					for (Element<?> child : TypeUtils.getAllChildren(service.getParameters())) {
						// we don't want to update the primary, but we do need to keep track of the name of the field so we can use it to target the update
						Value<Boolean> primary = child.getProperty(PrimaryKeyProperty.getInstance());
						if (primary != null && primary.getValue()) {
							idField = child.getName();
							continue;
						}
						else if (idField == null && child.getName().equalsIgnoreCase("id")) {
							idField = child.getName();
							continue;
						}
						
						// if it is generated, we don't update it by default
						Value<Boolean> property = child.getProperty(GeneratedProperty.getInstance());
						if (property != null && property.getValue() != null && property.getValue()) {
							continue;
						}
						if (!sql.toString().isEmpty()) {
							sql.append(",\n");
						}
						sql.append("\t" + EAIRepositoryUtils.uncamelify(child.getName()) + " = case when :" + child.getName() + " is null then " + EAIRepositoryUtils.uncamelify(child.getName()) + " else :" + child.getName() + " end");
					}
					String result = "update ~" + EAIRepositoryUtils.uncamelify(JDBCUtils.getTypeName(service.getParameters(), true)) + " set\n" + sql.toString() + "\n where " + (idField == null ? "<query>" : EAIRepositoryUtils.uncamelify(idField) + " = :" + idField);
//					target.textProperty().set(result);
					editor.setContent("text/sql", result);
					button.setText("Update");
					button.setGraphic(MainController.loadGraphic("edit-button.png"));
				}
				else {
					StringBuilder sql = new StringBuilder();
					String idField = null;
					for (Element<?> child : TypeUtils.getAllChildren(service.getParameters())) {
						// we don't want to update the primary, but we do need to keep track of the name of the field so we can use it to target the update
						Value<Boolean> primary = child.getProperty(PrimaryKeyProperty.getInstance());
						if (primary != null && primary.getValue()) {
							idField = child.getName();
							continue;
						}
						else if (idField == null && child.getName().equalsIgnoreCase("id")) {
							idField = child.getName();
							continue;
						}
						
						// if it is generated, we don't update it by default
						Value<Boolean> property = child.getProperty(GeneratedProperty.getInstance());
						if (property != null && property.getValue() != null && property.getValue()) {
							continue;
						}
						if (!sql.toString().isEmpty()) {
							sql.append(",\n");
						}
						sql.append("\t" + EAIRepositoryUtils.uncamelify(child.getName()) + " = :" + child.getName());
					}
					String result = "update ~" + EAIRepositoryUtils.uncamelify(JDBCUtils.getTypeName(service.getParameters(), true)) + " set\n" + sql.toString() + "\n where " + (idField == null ? "<query>" : EAIRepositoryUtils.uncamelify(idField) + " = :" + idField);
//					target.textProperty().set(result);
					editor.setContent("text/sql", result);
					button.setText("Merge Update");
					button.setGraphic(MainController.loadGraphic("edit-button.png"));
				}
				MainController.getInstance().setChanged();
			}
		});
	}

	private void generateSelect(Button button, final JDBCService service, final TextArea target, final AceEditor editor) {
		button.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent event) {
				if (button.getText().contains("Join")) {
					StringBuilder sql = new StringBuilder();
					List<ComplexType> types = new ArrayList<ComplexType>();
					ComplexType result = service.getResults();
					while (result != null) {
						types.add(result);
						result = (ComplexType) result.getSuperType();
					}
					Collections.reverse(types);
					List<Element<?>> inherited = new ArrayList<Element<?>>();
					Map<ComplexType, String> names = JDBCServiceManager.generateNames(types);
					for (ComplexType type : types) {
						Boolean value = ValueUtils.getValue(HiddenProperty.getInstance(), type.getProperties());
						if (!inherited.isEmpty() && (value == null || !value)) {
							for (Element<?> child : inherited) {
								if (!sql.toString().isEmpty()) {
									sql.append(",\n");
								}
								sql.append("\t" + names.get(type) + "." + EAIRepositoryUtils.uncamelify(child.getName()));
							}
							inherited.clear();
						}
						for (Element<?> child : type) {
							// if this is not the same child as we get back from the top level, we probably restricted it at some point
							if (!child.equals(service.getResults().get(child.getName()))) {
								continue;
							}
							if (value != null && value) {
								inherited.add(child);
							}
							else {
								if (!sql.toString().isEmpty()) {
									sql.append(",\n");
								}
								sql.append("\t" + names.get(type) + "." + EAIRepositoryUtils.uncamelify(child.getName()));
							}
						}
					}
					StringBuilder from = new StringBuilder();
					ComplexType previous = null;
					String previousCollectionName = null;
					for (ComplexType type : types) {
						Boolean value = ValueUtils.getValue(HiddenProperty.getInstance(), type.getProperties());
						if (value != null && value) {
							continue;
						}
						String typeName = EAIRepositoryUtils.uncamelify(JDBCUtils.getTypeName(type, true));
						// already bound, no additional binding necessary!
						if (previousCollectionName != null && previousCollectionName.equals(typeName)) {
							continue;
						}
						if (previous != null) {
							String previousName = names.get(previous);
							List<String> binding = JDBCUtils.getBinding(type, previous);
							from.append(" join ~" + typeName + " " + names.get(type)).append(" on " + names.get(type) + "." + EAIRepositoryUtils.uncamelify(binding.get(0)) + " = " + previousName + "." + EAIRepositoryUtils.uncamelify(binding.get(1)));
						}
						else {
							from.append(" ~").append(typeName + " " + names.get(type));
						}
						previous = type;
						previousCollectionName = typeName;
					}
					String string = "select\n" + sql.toString() + "\nfrom" + from.toString();
//					target.textProperty().set(string);
					editor.setContent("text/sql", string);
					button.setText("Select");
					button.setGraphic(MainController.loadGraphic("edit-button.png"));
				}
				else {
					Map<ComplexType, String> names = JDBCServiceManager.generateNames(Arrays.asList(service.getResults()));
					String name = names.values().iterator().next();
					StringBuilder sql = new StringBuilder();
					for (Element<?> child : TypeUtils.getAllChildren(service.getResults())) {
						if (!sql.toString().isEmpty()) {
							sql.append(",\n");
						}
						sql.append("\t " + name + "." + EAIRepositoryUtils.uncamelify(child.getName()));
					}
					String result = "select\n" + EAIRepositoryUtils.uncamelify(sql.toString()) + "\nfrom ~" + EAIRepositoryUtils.uncamelify(JDBCUtils.getTypeName(service.getResults(), true)) + " " + name;
//					target.textProperty().set(result);
					editor.setContent("text/sql", result);
					if (service.getResults().getSuperType() != null) {
						button.setText("Join Select");
						button.setGraphic(MainController.loadGraphic("edit-button.png"));
					}
				}
				MainController.getInstance().setChanged();
			}
		});
	}

	void syncBeforeSave(JDBCService service) {
//		MainController.getInstance().notify(service.setSql(area.getText()).toArray(new ValidationMessage[0]));
		MainController.getInstance().notify(service.setSql(editor.getContent()).toArray(new ValidationMessage[0]));
	}
}
