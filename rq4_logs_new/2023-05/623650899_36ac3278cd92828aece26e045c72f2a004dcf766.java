package MainStage.Components.FlowsDefinition.SubComponents;

import MainStage.Components.FlowsDefinition.FlowsDefinitionController;
import StepConnections.InputConnections;
import StepConnections.OutputConnections;
import Steps.StepDescriptor;
import javafx.beans.property.SimpleStringProperty;
import javafx.fxml.FXML;
import javafx.geometry.Point2D;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;
import javafx.scene.input.MouseEvent;

public class FlowsDefinitionStepToolTipLabelController {

    @FXML
    private Label stepLabel;
    private Tooltip stepToolTip;

    private SimpleStringProperty stepLabelText;
    private SimpleStringProperty toolTipText;

    private FlowsDefinitionController flowsDefinitionController;

    @FXML
    public void initialize(){
        stepLabelText=new SimpleStringProperty();
        stepLabel.textProperty().bind(stepLabelText);
        stepToolTip=new Tooltip("Step Description");
        toolTipText=new SimpleStringProperty();
        stepToolTip.textProperty().bind(toolTipText);
    }

    @FXML
    void stepLabelMouseEntered(MouseEvent event) {
        Point2D mousePosition = stepLabel.localToScreen(event.getX(), event.getY());
        stepToolTip.show(stepLabel, mousePosition.getX() + 10, mousePosition.getY());
    }

    @FXML
    void stepLabelMouseExited(MouseEvent event) {
        stepToolTip.hide();
    }

    public void setFlowsDefinitionController(FlowsDefinitionController flowsDefinitionController){
        this.flowsDefinitionController=flowsDefinitionController;
    }

    public void setStepLabelToolTipText(StepDescriptor stepDescriptor){
        stepLabelText.set(stepDescriptor.getStepEffectiveName());
        String text="";
        if(!stepDescriptor.getInputConnections().isEmpty()) {
            text = stepDescriptor.getStepEffectiveName() + "\ninputs:\n";
            for (String inputConnectionsString : stepDescriptor.getInputConnections().keySet()) {
                InputConnections inputConnections = stepDescriptor.getInputConnections().get(inputConnectionsString);
                text += inputConnections.getInputName() + " <- " + inputConnections.getConnectedOutputName() + ", " + inputConnections.getConnectedStepName() + "\n";
            }
        }

        if(!stepDescriptor.getOutputConnections().isEmpty())
        {
            text += "outputs:\n";
            for (String outputConnectionsString : stepDescriptor.getOutputConnections().keySet()) {
                OutputConnections outputConnections = stepDescriptor.getOutputConnections().get(outputConnectionsString);
                text += outputConnections.getOutputName() + " -> ";
                for(int i=0;i<outputConnections.getConnectedInputsName().size();i++){
                    text+=outputConnections.getConnectedInputsName().get(i)+", "+outputConnections.getConnectedStepsName().get(i);
                    text+=" and ";
                }
                text=text.substring(0,text.length()-5)+"\n";
            }
        }

        toolTipText.set(text);
    }

}