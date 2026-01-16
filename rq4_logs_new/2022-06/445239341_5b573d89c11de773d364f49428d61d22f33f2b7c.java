package password.vault.client.gui.controllers;

import javafx.application.Platform;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.DialogPane;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.stage.Modality;
import javafx.stage.Window;
import password.vault.client.gui.CommonUIElements;

import java.io.IOException;

public class GetMasterPasswordDialogController extends Dialog<String> {
    @FXML
    private DialogPane dialogGetMasterPassword;

    @FXML
    private GridPane gridData;

    @FXML
    private Label lblMasterPassword;

    @FXML
    private TextField txtPasswordShown;

    @FXML
    private PasswordField txtPassword;

    @FXML
    private CheckBox chBoxShowPassword;

    @FXML
    private Label lblErrors;

    public GetMasterPasswordDialogController(Window owner) {
        try {
            FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/password/vault/client/gui/controllers" +
                                                                                  "/get_master_password_dialog.fxml"));
            fxmlLoader.setController(this);

            fxmlLoader.load();

            setDialogPane(dialogGetMasterPassword);
            setResultConverter(dialogButton -> {
                if (dialogButton == ButtonType.OK) {
                    return txtPassword.getText();
                } else {
                    return null;
                }
            });

            final Button okButton = (Button) dialogGetMasterPassword.lookupButton(ButtonType.OK);
            okButton.addEventFilter(ActionEvent.ACTION, ae -> {
                if (!fieldsAreValid()) {
                    lblErrors.setVisible(true);
                    lblErrors.setText("error : all fields are necessary!");
                    ae.consume(); //not valid
                }
            });

            initOwner(owner);
            initModality(Modality.APPLICATION_MODAL);
            setOnShowing(dialogEvent -> Platform.runLater(() -> txtPassword.requestFocus()));

        } catch (IOException exception) {
            throw new RuntimeException("unable to load custom add credential class", exception);
        }
    }

    @FXML
    void initialize() {
        CommonUIElements.setupShowHidePasswordCheckbox(txtPasswordShown, txtPassword, chBoxShowPassword);
    }

    private boolean fieldsAreValid() {
        return !txtPassword.getText().isBlank();
    }
}