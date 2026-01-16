package password.vault.client.gui.controllers;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.DialogPane;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.stage.Modality;
import javafx.stage.Window;
import password.vault.client.gui.model.CredentialAdditionRequest;

import java.io.IOException;

/**
 * source : <a href="https://stackoverflow.com/a/64967696/9127495">...</a>
 */
public class AddCredentialDialogPaneController extends Dialog<CredentialAdditionRequest> {

    @FXML
    private DialogPane dialogAddCredentials;

    @FXML
    private TextField txtPasswordShown;

    @FXML
    private PasswordField txtPassword;

    @FXML
    private CheckBox chBoxShowPassword;

    @FXML
    private TextField txtWebsite;

    @FXML
    private TextField txtUsername;

    @FXML
    private TextField txtMasterPasswordShown;

    @FXML
    private PasswordField txtMasterPassword;

    @FXML
    private CheckBox chBoxShowMasterPassword;

    public AddCredentialDialogPaneController(Window owner) {
        try {
            FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/password/vault/client/gui/controllers" +
                                                                                  "/add_credential_dialog_pane.fxml"));
            fxmlLoader.setController(this);

            fxmlLoader.load();

            setDialogPane(dialogAddCredentials);
            setResultConverter(dialogButton -> {
                if (dialogButton == ButtonType.OK) {
                    return new CredentialAdditionRequest(txtWebsite.getText(), txtUsername.getText(),
                                                         txtPassword.getText(), txtMasterPassword.getText());
                }
                return null;
            });

            initOwner(owner);
            initModality(Modality.APPLICATION_MODAL);
            setOnShowing(dialogEvent -> Platform.runLater(() -> txtWebsite.requestFocus()));

        } catch (IOException exception) {
            throw new RuntimeException("unable to load custom add credential class", exception);
        }
    }

    @FXML
    void initialize() {
    }

    public DialogPane getDialogAddCredentials() {
        return dialogAddCredentials;
    }

}