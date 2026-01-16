package add;

import com.jfoenix.controls.JFXButton;
import com.jfoenix.controls.JFXComboBox;
import com.jfoenix.controls.JFXTextArea;
import com.jfoenix.controls.JFXTextField;

import java.net.URL;
import java.util.ResourceBundle;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Label;

public class AddController {

    @FXML
    private ResourceBundle resources;

    @FXML
    private URL location;

    @FXML
    private JFXTextField addTxtUrl;

    @FXML
    private JFXTextField addTxtTitle;

    @FXML
    private JFXTextField addTxtTags;

    @FXML
    private JFXComboBox<?> addComboEnv;

    @FXML
    private JFXTextArea addTxtDesc;

    @FXML
    private Label addLblAdded;

    @FXML
    private JFXButton addBtnCancel;

    @FXML
    private JFXButton addBtnAdd;

    @FXML
    void handleAdd(ActionEvent event) {

    }

    @FXML
    void handleCancel(ActionEvent event) {

    }

    @FXML
    void initialize() {
        checkLoadSuccess();

    }

    private void checkLoadSuccess() {
        assert addTxtUrl != null : "fx:id=\"addTxtUrl\" was not injected: check your FXML file 'add.fxml'.";
        assert addTxtTitle != null : "fx:id=\"addTxtTitle\" was not injected: check your FXML file 'add.fxml'.";
        assert addTxtTags != null : "fx:id=\"addTxtTags\" was not injected: check your FXML file 'add.fxml'.";
        assert addComboEnv != null : "fx:id=\"addComboEnv\" was not injected: check your FXML file 'add.fxml'.";
        assert addTxtDesc != null : "fx:id=\"addTxtDesc\" was not injected: check your FXML file 'add.fxml'.";
        assert addLblAdded != null : "fx:id=\"addLblAdded\" was not injected: check your FXML file 'add.fxml'.";
        assert addBtnCancel != null : "fx:id=\"addBtnCancel\" was not injected: check your FXML file 'add.fxml'.";
        assert addBtnAdd != null : "fx:id=\"addBtnAdd\" was not injected: check your FXML file 'add.fxml'.";
    }
}