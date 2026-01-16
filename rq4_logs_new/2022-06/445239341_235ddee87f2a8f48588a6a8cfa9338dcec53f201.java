package password.vault.client.gui.controllers;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.VBox;

import java.io.IOException;

public class Credential extends VBox {

    @FXML
    private Label lblDomain;

    @FXML
    private Label lblUsername;

    @FXML
    private Button btnCopyPassword;

    @FXML
    private Button btnRemovePassword;


    public Credential() {
        FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/password/vault/client/gui/controllers" +
                                                                              "/credential.fxml"));
        fxmlLoader.setRoot(this);
        fxmlLoader.setController(this);

        try {
            fxmlLoader.load();
        } catch (IOException exception) {
            throw new RuntimeException("unable to load custom vbox class", exception);
        }
    }

    @FXML
    void btnCopyPasswordClicked(ActionEvent event) {

    }

    @FXML
    void btnRemovePasswordClicked(ActionEvent event) {

    }

    public Label getLblDomain() {
        return lblDomain;
    }

    public void setLblDomain(Label lblDomain) {
        this.lblDomain = lblDomain;
    }

    public Label getLblUsername() {
        return lblUsername;
    }

    public void setLblUsername(Label lblUsername) {
        this.lblUsername = lblUsername;
    }
}