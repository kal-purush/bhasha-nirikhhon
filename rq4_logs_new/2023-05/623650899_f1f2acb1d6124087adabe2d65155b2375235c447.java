package MainStage.Components.FlowsExecution.SubComponents.InputGUI;

import MainStage.Components.FlowsExecution.FlowsExecutionController;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;

public class InputGUIController {

    @FXML
    private TextField inputTextField;
    @FXML
    private Label inputNameLabel;

    private FlowsExecutionController flowsExecutionController;

    private SimpleStringProperty inputName;

    @FXML
    public void initialize(){
        inputName=new SimpleStringProperty();
        inputNameLabel.textProperty().bind(inputName);
    }

    public void setFlowsExecutionController(FlowsExecutionController flowsExecutionController) {
        this.flowsExecutionController = flowsExecutionController;
    }

    public void setInputLabel(String name){
        inputName.set(name);
    }

    public SimpleBooleanProperty isTextFieldFilled(){
        return new SimpleBooleanProperty(inputTextField.textProperty().get()!="");
    }
}