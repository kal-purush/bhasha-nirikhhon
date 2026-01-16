package MainStage.Components.FlowsDefinition.SubComponents;

import Flow.FlowDescriptor;
import MainStage.Components.FlowsDefinition.FlowsDefinitionController;
import javafx.beans.property.SimpleStringProperty;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;

public class FlowDefinitionButtonController {

    @FXML
    private Button flowButton;
    @FXML
    private Label flowNameLabel;
    @FXML
    private Label flowDescriptionLabel;
    @FXML
    private Label numberOfStepsLabel;
    @FXML
    private Label numberOfFreeInputsLabel;
    @FXML
    private Label numberOfContinuationsLabel;

    private FlowsDefinitionController flowsDefinitionController;
    private SimpleStringProperty flowNameStringProperty;
    private SimpleStringProperty flowDescriptionStringProperty;
    private SimpleStringProperty numberOfStepsStringProperty;
    private SimpleStringProperty numberOfFreeInputsStringProperty;
    private SimpleStringProperty numberOfContinuationsStringProperty;

    @FXML
    public void initialize(){
        this.flowNameStringProperty=new SimpleStringProperty();
        flowNameLabel.textProperty().bind(flowNameStringProperty);
        this.flowDescriptionStringProperty=new SimpleStringProperty();
        flowDescriptionLabel.textProperty().bind(flowDescriptionStringProperty);
        this.numberOfStepsStringProperty=new SimpleStringProperty();
        numberOfStepsLabel.textProperty().bind(numberOfStepsStringProperty);
        this.numberOfFreeInputsStringProperty=new SimpleStringProperty();
        numberOfFreeInputsLabel.textProperty().bind(numberOfFreeInputsStringProperty);
        this.numberOfContinuationsStringProperty=new SimpleStringProperty();
        numberOfContinuationsLabel.textProperty().bind(numberOfContinuationsStringProperty);
    }

    @FXML
    void flowButtonAction(ActionEvent event) {
        flowsDefinitionController.showFlowDetails(flowNameStringProperty);
    }

    public void setFlowsDefinitionsController(FlowsDefinitionController flowsDefinitionController){
        this.flowsDefinitionController = flowsDefinitionController;
    }

    public void setFlowButtonText(FlowDescriptor flowDescriptor){
        flowNameStringProperty.set("Flow Name: "+flowDescriptor.getFlowName());
        flowDescriptionStringProperty.set("Flow Description: "+flowDescriptor.getFlowDescription());
        numberOfStepsStringProperty.set("Number Of Steps: "+flowDescriptor.getStepDescriptors().size());
        numberOfFreeInputsStringProperty.set("Number Of Free Inputs: "+flowDescriptor.getFreeInputs().size());
        numberOfContinuationsStringProperty.set("Number Of Continuations: "+"to be added");
    }



}