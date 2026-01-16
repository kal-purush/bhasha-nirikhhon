package add;

import com.jfoenix.controls.JFXButton;
import com.jfoenix.controls.JFXColorPicker;
import com.jfoenix.controls.JFXTextArea;
import com.jfoenix.controls.JFXTextField;

import java.sql.SQLException;
import java.util.ResourceBundle;
import java.util.logging.Level;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.paint.Color;
import javafx.stage.Stage;
import manage.Manager;
import model.Environment;
import model.Tag;

public class AddEnvironmentController {

    private Stage dialogStage;

    @FXML
    private JFXTextField addTxtName;
    @FXML
    private ResourceBundle resources;

    @FXML
    private JFXColorPicker addClrColor;

    @FXML
    private JFXTextArea addTxtDesc;

    @FXML
    private JFXButton addBtnCancel;

    @FXML
    private JFXButton addBtnAdd;

    /**
     * Eventhandler for {@link #addBtnAdd}. When the Button is clicked, it checks if the information is complete.
     * Then it creates a new Bookmark with the given information.
     * For adding the correct references of @{@link Tag} to the bookmark it uses the Tag.add Method.
     * Then the Bookmark will be added to the Bookmark list and the stage closed.
     *
     * @param event Button clicked event.
     */
    @FXML
    void handleAdd(ActionEvent event) {
        if (checkFields()) {
            String name = addTxtName.getText();
            String desc = addTxtDesc.getText();
            Color color = addClrColor.getValue();
            try {
                Environment.add(new Environment(Environment.getNewID(), name, desc, color));
            } catch (SQLException e) {
                Manager.alertException(resources.getString("error"),
                        resources.getString("error.02"),
                        e.getMessage(), this.dialogStage,
                        e,
                        Level.SEVERE);
            }
            dialogStage.close();
        } else {
            Manager.alertWarning(resources.getString("add.invalid"),
                    resources.getString("add.invalid"),
                    resources.getString("add.invalid.content"),
                    this.dialogStage);
        }
    }


    /**
     * Checks if all required fields are filled.
     *
     * @return true if all required fiels are filled, else false.
     */
    private boolean checkFields() {
        boolean ok = true;
        if (addTxtName.getText().equals("")) ok = false;//name
        if (addTxtDesc.getText().equals("")) ok = false;//desc
        if (addClrColor.getValue() == null) ok = false;//color
        return ok;
    }

    @FXML
    void handleCancel(ActionEvent event) {
        dialogStage.close();
    }

    @FXML
    public void initialize() {

    }

    public void setDialogStage(Stage dialogStage) {
        this.dialogStage = dialogStage;
    }
}