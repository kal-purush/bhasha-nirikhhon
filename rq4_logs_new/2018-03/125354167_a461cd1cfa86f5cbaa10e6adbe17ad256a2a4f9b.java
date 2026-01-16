package add;

import com.jfoenix.controls.JFXButton;
import com.jfoenix.controls.JFXComboBox;
import com.jfoenix.controls.JFXTextArea;
import com.jfoenix.controls.JFXTextField;

import java.net.URL;
import java.time.LocalDateTime;
import java.util.ResourceBundle;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.stage.Stage;
import manage.Manager;
import model.Bookmark;
import model.Environment;
import model.Tag;

public class AddBookmarkController {

    /**
     * Reference to the Manager that initialized the controller
     */
    private Manager manager;
    /**
     * The actual Dialog Stage, the ok and cancel button must have the ability to interact with it.
     */
    private Stage dialogStage;

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
    private JFXComboBox<Environment> addComboEnv;

    @FXML
    private JFXTextArea addTxtDesc;

    @FXML
    private Label addLblAdded;

    @FXML
    private JFXButton addBtnCancel;

    @FXML
    private JFXButton addBtnAdd;

    @FXML
//TODO javadoc
    void handleAdd(ActionEvent event) {
        if (checkFields()) {
            String title = addTxtTitle.getText();
            String url = addTxtUrl.getText();
            String desc = addTxtDesc.getText();
            Environment env = addComboEnv.getSelectionModel().getSelectedItem();
            Bookmark bookmark = new Bookmark(-1, desc, title, url, LocalDateTime.now(), env, Tag.add(addTxtTags.getText()));
            Bookmark.getBookmarks().add(bookmark);
            //TODO when the application closes, all tags, bookmarks and environments should be written to the database when the id equals -1
            dialogStage.close();
        }
    }

    //TODO javadoc
    private boolean checkFields() {
        boolean ok = true;
        if (addTxtUrl.getText().equals("")) ok = false;//url
        if (addTxtTitle.getText().equals("")) ok = false;//title
        if (addTxtTags.getText().equals("")) ok = false;//tags
        if (addTxtDesc.getText().equals("")) ok = false;//desc
//        if (addComboEnv.getSelectionModel().getSelectedItem().equals("")) ok = false;//env
        return ok;
    }

    @FXML
    void handleCancel(ActionEvent event) {

    }

    /**
     * initialize the view. set the actual Date and Time to the added Label.
     */
    @FXML
    void initialize() {
        addLblAdded.setText(LocalDateTime.now().toString());//Now
        addComboEnv.setItems(Environment.getEnvironments());
    }


    /**
     * Setter
     *
     * @param manager Reference to the Manager
     */
    public void setManager(Manager manager) {
        this.manager = manager;
    }

    /**
     * Setter
     *
     * @param dialogStage Refernce to the Dialog stage
     */
    public void setDialogStage(Stage dialogStage) {
        this.dialogStage = dialogStage;
    }
}