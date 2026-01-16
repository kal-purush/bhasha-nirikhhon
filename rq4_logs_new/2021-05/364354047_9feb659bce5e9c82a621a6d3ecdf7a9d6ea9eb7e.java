package sample;


import javafx.fxml.FXML;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;

public class Controller {
    @FXML
    TextArea mainTextArea;

    @FXML
    TextField textField;


    public void btnClickAction() {
        if(!textField.getText().equals("")) {
            mainTextArea.appendText(textField.getText() +"\n" + "\n");
            textField.setText("");
        }
    }

    public void tfClickAction() {
        if(!textField.getText().equals("")) {
            mainTextArea.appendText(textField.getText() + "\n" + "\n");
            textField.setText("");
        }
    }

}