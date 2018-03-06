package be.nabu.eai.module.services.jdbc.stored;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import nabu.services.jdbc.types.StoredProcedureInterface;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TextField;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.util.Callback;
import be.nabu.eai.developer.MainController;
import be.nabu.eai.developer.managers.base.BaseArtifactGUIInstance;
import be.nabu.eai.developer.managers.base.BasePortableGUIManager;
import be.nabu.eai.developer.managers.util.ElementMarshallable;
import be.nabu.eai.developer.managers.util.SimpleProperty;
import be.nabu.eai.developer.util.EAIDeveloperUtils;
import be.nabu.eai.developer.util.ElementClipboardHandler;
import be.nabu.eai.developer.util.ElementSelectionListener;
import be.nabu.eai.developer.util.ElementTreeItem;
import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.ModifiableEntry;
import be.nabu.eai.repository.api.ResourceEntry;
import be.nabu.eai.repository.resources.RepositoryEntry;
import be.nabu.jfx.control.tree.Tree;
import be.nabu.libs.artifacts.api.DataSourceProviderArtifact;
import be.nabu.libs.property.api.Property;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceResult;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.base.RootElement;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.libs.types.properties.TimezoneProperty;

public class StoredProcedureGUIManager extends BasePortableGUIManager<StoredProcedureArtifact, BaseArtifactGUIInstance<StoredProcedureArtifact>> {

	public StoredProcedureGUIManager() {
		super("Stored Procedure", StoredProcedureArtifact.class, new StoredProcedureManager());
	}

	@Override
	public String getCategory() {
		return "Services";
	}
	
	private void drawInterface(StoredProcedureArtifact artifact, SplitPane split) {
		split.getItems().clear();
		
		ElementSelectionListener elementSelectionListener = new ElementSelectionListener(MainController.getInstance(), false, true, FormatProperty.getInstance(), TimezoneProperty.getInstance(), MinOccursProperty.getInstance(), MaxOccursProperty.getInstance(), PrimaryKeyProperty.getInstance(), CollectionNameProperty.getInstance());
		elementSelectionListener.setForceAllowUpdate(true);
		
		Tree<Element<?>> input = new Tree<Element<?>>(new ElementMarshallable());
		input.rootProperty().set(new ElementTreeItem(new RootElement(artifact.getServiceInterface().getInputDefinition()), null, false, false));
		input.getRootCell().expandAll(2);
		
		Tree<Element<?>> output = new Tree<Element<?>>(new ElementMarshallable());
		output.rootProperty().set(new ElementTreeItem(new RootElement(artifact.getServiceInterface().getOutputDefinition()), null, false, false));
		output.getRootCell().expandAll(2);
		
		EAIDeveloperUtils.addElementExpansionHandler(input);
//		input.getSelectionModel().selectedItemProperty().addListener(new ElementSelectionListener(MainController.getInstance(), false));
		input.getSelectionModel().selectedItemProperty().addListener(elementSelectionListener);
		input.setClipboardHandler(new ElementClipboardHandler(input, false));
		
		EAIDeveloperUtils.addElementExpansionHandler(output);
//		output.getSelectionModel().selectedItemProperty().addListener(new ElementSelectionListener(MainController.getInstance(), false));
		output.getSelectionModel().selectedItemProperty().addListener(elementSelectionListener);
		output.setClipboardHandler(new ElementClipboardHandler(output, false));
		
		ScrollPane inputScroll = new ScrollPane();
		ScrollPane outputScroll = new ScrollPane();
		inputScroll.setContent(input);
		outputScroll.setContent(output);
		output.prefWidthProperty().bind(outputScroll.widthProperty());
		input.prefWidthProperty().bind(inputScroll.widthProperty());
		split.getItems().addAll(inputScroll, outputScroll);
	}
	
	private void updateLabel(StoredProcedureArtifact artifact, Label label) {
		label.setText((artifact.getConfig().getSchema() == null ? "" : artifact.getConfig().getSchema() + ".") + (artifact.getConfig().getName() == null ? "" : artifact.getConfig().getName())
			+ (artifact.getConfig().getUniqueName() == null ? "" : " (" + artifact.getConfig().getUniqueName() + ")"));
	}
	
	@Override
	public void display(MainController controller, AnchorPane pane, StoredProcedureArtifact artifact) throws IOException, ParseException {
		VBox box = new VBox();
		
		SplitPane split = new SplitPane();
		drawInterface(artifact, split);
		
		HBox buttons = new HBox();
		
		Label label = new Label();
		updateLabel(artifact, label);
		
		// need a button to refresh the current stored procedure (update input/output)
		// and a button to browse for a new one
		Button browse = new Button("Select Stored Procedure");
		browse.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
			public void handle(ActionEvent arg0) {
				try {
					Service storedProcedures = (Service) EAIResourceRepository.getInstance().resolve("nabu.services.jdbc.Services.storedProcedures");
					ComplexContent input = storedProcedures.getServiceInterface().getInputDefinition().newInstance();
					input.set("connectionId", artifact.getConfig().getConnection().getId());
					Future<ServiceResult> run = EAIResourceRepository.getInstance().getServiceRunner().run(
						storedProcedures, 
						EAIResourceRepository.getInstance().newExecutionContext(null), 
						input
					);
					ServiceResult serviceResult = run.get();
					if (serviceResult.getException() != null) {
						throw serviceResult.getException();
					}
					ComplexContent output = serviceResult.getOutput();
					List object = (List) output.get("storedProcedures");
					if (object == null) {
						throw new IllegalArgumentException("No stored procedures found");
					}
					VBox box = new VBox();
					HBox buttons = new HBox();
					ListView view = new ListView(FXCollections.observableArrayList(object));
					TextField search = new TextField();
					search.textProperty().addListener(new ChangeListener<String>() {
						@Override
						public void changed(ObservableValue<? extends String> arg0, String arg1, String arg2) {
							view.getItems().clear();
							if (arg2 == null || arg2.trim().isEmpty()) {
								view.getItems().addAll(object);
							}
							else {
								arg2 = arg2.toLowerCase();
								for (Object element : object) {
									if (!(element instanceof ComplexContent)) {
										element = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(element);
									}
									ComplexContent content = (ComplexContent) element;
									String schema = (String) content.get("schema");
									String name = (String) content.get("name");
									String uniqueName = (String) content.get("uniqueName");
									if (schema.toLowerCase().indexOf(arg2) >= 0 || name.toLowerCase().indexOf(arg2) >= 0 || uniqueName.toLowerCase().indexOf(arg2) >= 0) {
										view.getItems().add(element);
									}
								}
							}
						}
					});
					view.setCellFactory(new Callback<ListView, ListCell>() {
						@Override
						public ListCell call(ListView arg0) {
							return new ListCell() {
								public void updateItem(Object item, boolean empty) {
									super.updateItem(item, empty);
									if (item != null) {
										if (!(item instanceof ComplexContent)) {
											item = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(item);
										}
										ComplexContent content = (ComplexContent) item;
										setText(content.get("schema") + "." + content.get("name") + " (" + content.get("uniqueName") + ")");
									}
									else {
										setText("");
									}
								}
							};
						}
					});
					box.getChildren().addAll(search, view, buttons);
					
					Button choose = new Button("Choose");
					choose.disableProperty().bind(view.getSelectionModel().selectedItemProperty().isNull());
					
					Button close = new Button("Cancel");
					box.minWidthProperty().set(400);
					Stage buildPopup = EAIDeveloperUtils.buildPopup("Available Procedures", box, true);
					close.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
						@Override
						public void handle(ActionEvent arg0) {
							buildPopup.hide();
						}
					});
					choose.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
						@Override
						public void handle(ActionEvent arg0) {
							Object selectedItem = view.getSelectionModel().getSelectedItem();
							if (!(selectedItem instanceof ComplexContent)) {
								selectedItem = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(selectedItem);
							}
							ComplexContent content = (ComplexContent) selectedItem;
							artifact.getConfig().setCatalogue((String) content.get("catalogue"));
							artifact.getConfig().setSchema((String) content.get("schema"));
							artifact.getConfig().setName((String) content.get("name"));
							artifact.getConfig().setUniqueName((String) content.get("uniqueName"));
							
							Service storedProcedureInterface = (Service) EAIResourceRepository.getInstance().resolve("nabu.services.jdbc.Services.storedProcedureInterface");
							ComplexContent input = storedProcedureInterface.getServiceInterface().getInputDefinition().newInstance();
							input.set("connectionId", artifact.getConfig().getConnection().getId());
							input.set("catalogue", artifact.getConfig().getCatalogue());
							input.set("name", artifact.getConfig().getName());
							input.set("uniqueName", artifact.getConfig().getUniqueName());
							Future<ServiceResult> run = EAIResourceRepository.getInstance().getServiceRunner().run(
								storedProcedureInterface, 
								EAIResourceRepository.getInstance().newExecutionContext(null), 
								input
							);
							try {
								ServiceResult serviceResult = run.get();
								if (serviceResult.getException() != null) {
									throw serviceResult.getException();
								}
								ComplexContent output = serviceResult.getOutput();
								Object iface = output.get("storedProcedureInterface");
								if (iface != null) {
									if (iface instanceof BeanInstance) {
										iface = ((BeanInstance) iface).getUnwrapped();
									}
									artifact.getConfig().setParameters(((StoredProcedureInterface) iface).getParameters());
								}
								// reset the parameters
								else {
									artifact.getConfig().setParameters(null);
								}
								artifact.buildInterface();
								updateLabel(artifact, label);
								drawInterface(artifact, split);
								
								Entry entry = artifact.getRepository().getEntry(artifact.getId());
								((StoredProcedureManager) getArtifactManager()).refreshChildren((ModifiableEntry) entry, artifact);
								
								MainController.getInstance().setChanged();
							}
							catch (Exception e) {
								MainController.getInstance().notify(e);
							}
							buildPopup.close();
						}
					});
					buttons.getChildren().addAll(close, choose);
				}
				catch (Exception e) {
					MainController.getInstance().notify(e);
				}
			}
		});
		buttons.getChildren().addAll(label, browse);
		box.getChildren().addAll(buttons, split);
		
		pane.getChildren().add(box);
		
		AnchorPane.setBottomAnchor(box, 0d);
		AnchorPane.setLeftAnchor(box, 0d);
		AnchorPane.setRightAnchor(box, 0d);
		AnchorPane.setTopAnchor(box, 0d);
	}

	@Override
	protected List<Property<?>> getCreateProperties() {
		List<Property<?>> properties = new ArrayList<Property<?>>();
		SimpleProperty<DataSourceProviderArtifact> property = new SimpleProperty<DataSourceProviderArtifact>("Connection", DataSourceProviderArtifact.class, true);
		properties.add(property);
		return properties;
	}

	@Override
	protected BaseArtifactGUIInstance<StoredProcedureArtifact> newGUIInstance(Entry entry) {
		return new BaseArtifactGUIInstance<StoredProcedureArtifact>(this, entry);
	}

	@Override
	protected void setEntry(BaseArtifactGUIInstance<StoredProcedureArtifact> guiInstance, ResourceEntry entry) {
		guiInstance.setEntry(entry);
	}

	@Override
	protected StoredProcedureArtifact newInstance(MainController controller, RepositoryEntry entry, Value<?>...values) throws IOException {
		StoredProcedureArtifact artifact = new StoredProcedureArtifact(entry.getId(), entry.getContainer(), entry.getRepository());
		if (values == null || values.length == 0) {
			throw new RuntimeException("Missing connection");
		}
		for (Value<?> value : values) {
			if (value.getProperty().getName().equals("Connection")) {
				artifact.getConfig().setConnection((DataSourceProviderArtifact) value.getValue());
			}
		}
		return artifact;
	}

	@Override
	protected void setInstance(BaseArtifactGUIInstance<StoredProcedureArtifact> guiInstance, StoredProcedureArtifact instance) {
		guiInstance.setArtifact(instance);
	}

}
