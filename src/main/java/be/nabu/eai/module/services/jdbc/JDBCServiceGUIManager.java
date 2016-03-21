package be.nabu.eai.module.services.jdbc;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Orientation;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
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
import be.nabu.jfx.control.tree.Tree;
import be.nabu.jfx.control.tree.TreeItem;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.property.api.Property;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.base.RootElement;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.TimezoneProperty;
import be.nabu.libs.validator.api.ValidationMessage;
import be.nabu.libs.validator.api.ValidationMessage.Severity;

public class JDBCServiceGUIManager implements ArtifactGUIManager<JDBCService> {

	private TextArea area;

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
		EAIDeveloperUtils.buildPopup(controller, updater, "Create JDBC Service", new EventHandler<MouseEvent>() {
			@Override
			public void handle(MouseEvent arg0) {
				try {
					String name = updater.getValue("Name");
					RepositoryEntry entry = ((RepositoryEntry) target.itemProperty().get()).createNode(name, getArtifactManager(), true);
					JDBCService service = new JDBCService(entry.getId());
					getArtifactManager().save(entry, service);
					entry.getRepository().reload(target.itemProperty().get().getId());
					controller.getRepositoryBrowser().refresh();
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

		ElementSelectionListener elementSelectionListener = new ElementSelectionListener(controller, false, true, FormatProperty.getInstance(), TimezoneProperty.getInstance(), MaxOccursProperty.getInstance());
		elementSelectionListener.setForceAllowUpdate(true);
		
		ScrollPane left = new ScrollPane();
		VBox leftBox = new VBox();
		final Tree<Element<?>> input = new Tree<Element<?>>(new ElementMarshallable());
		input.rootProperty().set(new ElementTreeItem(new RootElement(service.getInput()), null, false, false));
		left.setContent(leftBox);
		input.prefWidthProperty().bind(left.widthProperty());
		input.getSelectionModel().selectedItemProperty().addListener(elementSelectionListener);
		
		CheckBox validateInput = new CheckBox();
		validateInput.setTooltip(new Tooltip("Validate Input"));
		validateInput.setSelected(service.getValidateInput() != null && service.getValidateInput());
		validateInput.selectedProperty().addListener(new ChangeListener<Boolean>() {
			@Override
			public void changed(ObservableValue<? extends Boolean> arg0, Boolean arg1, Boolean arg2) {
				service.setValidateInput(arg2);
				MainController.getInstance().setChanged();
			}
		});
		CheckBox validateOutput = new CheckBox();
		validateOutput.setTooltip(new Tooltip("Validate Output"));
		validateOutput.setSelected(service.getValidateOutput() != null && service.getValidateOutput());
		validateOutput.selectedProperty().addListener(new ChangeListener<Boolean>() {
			@Override
			public void changed(ObservableValue<? extends Boolean> arg0, Boolean arg1, Boolean arg2) {
				service.setValidateOutput(arg2);
				MainController.getInstance().setChanged();
			}
		});
		
		HBox namedInput = new HBox();
		namedInput.getChildren().addAll(validateInput, new Label("Input definition: "));
		TextField inputField = new TextField();
		final Button generateInsert = new Button("Generate Insert");
		generateInsert.disableProperty().set(service.isInputGenerated());
		final Button generateUpdate = new Button("Generate Update");
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
						controller.getTree().refresh();
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
						controller.getTree().refresh();
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
		output.rootProperty().set(new ElementTreeItem(new RootElement(service.getOutput()), null, false, false));
		right.setContent(rightBox);
		output.prefWidthProperty().bind(right.widthProperty());
		output.getSelectionModel().selectedItemProperty().addListener(elementSelectionListener);
		HBox namedOutput = new HBox();
		namedOutput.getChildren().addAll(validateOutput, new Label("Output definition: "));
		final Button generateSelect = new Button("Generate Select");
		generateSelect.disableProperty().set(service.isOutputGenerated());
		TextField outputField = new TextField();
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
						controller.getTree().refresh();
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
							controller.getTree().refresh();
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
				if (arg2 != null) {
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
			}
		});
		hbox.getChildren().addAll(new Label("Connection ID: "), field);
		
		HBox generatedColumnBox = new HBox();
		TextField generatedColumn = new TextField(service.getGeneratedColumn());
		generatedColumn.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> arg0, String arg1, String arg2) {
				service.setGeneratedColumn(arg2 == null || arg2.trim().isEmpty() ? null : arg2);
				MainController.getInstance().setChanged();
			}
		});
		generatedColumnBox.getChildren().addAll(new Label("Generated Column: "), generatedColumn);
		
		area = new TextArea();
		VBox.setVgrow(area, Priority.ALWAYS);
		area.focusedProperty().addListener(new ChangeListener<Boolean>() {
			@Override
			public void changed(ObservableValue<? extends Boolean> arg0, Boolean arg1, Boolean arg2) {
				if (!arg2) {
					controller.notify(service.setSql(area.getText()).toArray(new ValidationMessage[0]));
					getArtifactManager().refreshChildren((ModifiableEntry) entry, service);
					controller.getTree().refresh();
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
		vbox.getChildren().addAll(hbox, generatedColumnBox, area);
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
		
		generateInsert(generateInsert, service, area);
		generateUpdate(generateUpdate, service, area);
		generateSelect(generateSelect, service, area);
		
		return service;
	}
	
	
	private void generateInsert(Button button, final JDBCService service, final TextArea target) {
		button.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent event) {
				StringBuilder sql = new StringBuilder();
				for (Element<?> child : TypeUtils.getAllChildren(service.getParameters())) {
					if (!sql.toString().isEmpty()) {
						sql.append(",\n");
					}
					sql.append("\t" + child.getName());
				}
				target.textProperty().set("insert into " + EAIRepositoryUtils.uncamelify(service.getParameters().getName()) + " (\n" + EAIRepositoryUtils.uncamelify(sql.toString()) + "\n) values (\n" + sql.toString().replaceAll("([\\w]+)", ":$1") + "\n)");
				MainController.getInstance().setChanged();
			}
		});
	}
	
	private void generateUpdate(Button button, final JDBCService service, final TextArea target) {
		button.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent event) {
				if (button.getText().contains("Merge")) {
					StringBuilder sql = new StringBuilder();
					String idField = null;
					for (Element<?> child : TypeUtils.getAllChildren(service.getParameters())) {
						if (child.getName().equalsIgnoreCase("id")) {
							idField = child.getName();
							continue;
						}
						if (!sql.toString().isEmpty()) {
							sql.append(",\n");
						}
						sql.append("\t" + EAIRepositoryUtils.uncamelify(child.getName()) + " = case when :" + child.getName() + " is null then " + EAIRepositoryUtils.uncamelify(child.getName()) + " else :" + child.getName() + " end");
					}
					target.textProperty().set("update " + EAIRepositoryUtils.uncamelify(service.getParameters().getName()) + " set\n" + sql.toString() + "\n where " + (idField == null ? "<query>" : EAIRepositoryUtils.uncamelify(idField) + " = :" + idField));
					button.setText("Generate Update");
				}
				else {
					StringBuilder sql = new StringBuilder();
					String idField = null;
					for (Element<?> child : TypeUtils.getAllChildren(service.getParameters())) {
						if (child.getName().equalsIgnoreCase("id")) {
							idField = child.getName();
							continue;
						}
						if (!sql.toString().isEmpty()) {
							sql.append(",\n");
						}
						sql.append("\t" + EAIRepositoryUtils.uncamelify(child.getName()) + " = :" + child.getName());
					}
					target.textProperty().set("update " + EAIRepositoryUtils.uncamelify(service.getParameters().getName()) + " set\n" + sql.toString() + "\n where " + (idField == null ? "<query>" : EAIRepositoryUtils.uncamelify(idField) + " = :" + idField));
					button.setText("Generate Merge Update");
				}
				MainController.getInstance().setChanged();
			}
		});
	}
	
	private void generateSelect(Button button, final JDBCService service, final TextArea target) {
		button.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent event) {
				StringBuilder sql = new StringBuilder();
				for (Element<?> child : TypeUtils.getAllChildren(service.getResults())) {
					if (!sql.toString().isEmpty()) {
						sql.append(",\n");
					}
					sql.append("\t" + EAIRepositoryUtils.uncamelify(child.getName()));
				}
				target.textProperty().set("select\n" + EAIRepositoryUtils.uncamelify(sql.toString()) + "\nfrom " + EAIRepositoryUtils.uncamelify(service.getResults().getName()));
				MainController.getInstance().setChanged();
			}
		});
	}

	void syncBeforeSave(JDBCService service) {
		MainController.getInstance().notify(service.setSql(area.getText()).toArray(new ValidationMessage[0]));
	}
}
