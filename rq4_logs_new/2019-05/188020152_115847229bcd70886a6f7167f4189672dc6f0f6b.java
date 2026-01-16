package sample;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.control.*;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.control.TextArea;

import java.time.LocalDate;

public class AddnewWINDOWcontroller {

    @FXML
    public ComboBox<Priority> priority;

    @FXML
    public Button button2;

    @FXML
    public TextField title;

    @FXML
    public TextArea field;

    @FXML
    public DatePicker expdate = new DatePicker(null);

    @FXML
    public void initialize() {
        priority.getItems().addAll(Priority.ASAP,Priority.NOW,Priority.NEVER);
    }

    @FXML
    public void gotoTODO() {
        try {
            String mytitle = title.getText();
            String myfield = field.getText();
            LocalDate expdate =LocalDate.now();
            Priority mypriority = Priority.ASAP;
            Container name_val = new Container(mytitle,myfield,mypriority,expdate);
            name_val.title = name_val.getTitle();
            FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("sample.fxml"));
            Parent root = fxmlLoader.load();
            Controller myControllerHandle = fxmlLoader.getController();
            myControllerHandle.setToArea(name_val);
            button2.getScene().setRoot(root);

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}