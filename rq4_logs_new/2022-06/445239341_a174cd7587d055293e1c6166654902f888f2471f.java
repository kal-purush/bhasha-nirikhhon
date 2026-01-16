package password.vault.client.gui.components;

import javafx.application.Platform;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.DialogPane;
import javafx.scene.control.Label;
import javafx.scene.control.Slider;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.stage.Modality;
import javafx.stage.Window;
import password.vault.client.gui.model.FieldConstraints;
import password.vault.client.gui.model.GenerateCredentialsDialogResult;

import java.io.IOException;

public class GenerateCredentialDialogConroller extends Dialog<GenerateCredentialsDialogResult> {

    private static final String GENERATE_CREDENTIALS_DIALOG_FILENAME = "/password/vault/client/gui/components/" +
            "generate_credentials_dialog.fxml";

    @FXML
    private DialogPane dialogGenerateCredentials;

    @FXML
    private GridPane gridData;

    @FXML
    private Label lblWebsite;

    @FXML
    private Label lblUsername;

    @FXML
    private TextField txtWebsite;

    @FXML
    private TextField txtUsername;

    @FXML
    private Slider sliderLength;

    @FXML
    private Label lblSlider;

    @FXML
    private Label lblErrors;

    public GenerateCredentialDialogConroller(Window owner) {
        try {
            FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource(GENERATE_CREDENTIALS_DIALOG_FILENAME));
            fxmlLoader.setController(this);

            fxmlLoader.load();

            setDialogPane(dialogGenerateCredentials);
            setResultConverter(dialogButton -> {
                if (dialogButton == ButtonType.OK) {
                    return new GenerateCredentialsDialogResult(txtWebsite.getText(), txtUsername.getText(),
                                                               (int) sliderLength.getValue());
                }
                return null;
            });

            final Button okButton = (Button) dialogGenerateCredentials.lookupButton(ButtonType.OK);
            okButton.addEventFilter(ActionEvent.ACTION, actionEventFilter -> {
                if (!fieldsAreValid()) {
                    lblErrors.setVisible(true);
                    lblErrors.setText("error : all fields are necessary!");
                    actionEventFilter.consume();
                    return;
                }

                if (!isUsernameValid()) {
                    lblErrors.setVisible(true);
                    lblErrors.setText("error : username is not valid");
                    actionEventFilter.consume();
                    return;
                }

                if (!isWebsiteValid()) {
                    lblErrors.setVisible(true);
                    lblErrors.setText("error : website is not valid");
                    actionEventFilter.consume();
                    return;
                }
            });

            initOwner(owner);
            initModality(Modality.APPLICATION_MODAL);
            setOnShowing(dialogEvent -> Platform.runLater(() -> txtWebsite.requestFocus()));
        } catch (IOException exception) {
            throw new RuntimeException("unable to load custom dialog fxml file", exception);
        }
    }

    @FXML
    void initialize() {
        sliderLength.setBlockIncrement(1);
        sliderLength.setMajorTickUnit(1);
        sliderLength.setMinorTickCount(0);
        sliderLength.setShowTickLabels(true);
        sliderLength.setSnapToTicks(true);

    }

    private boolean fieldsAreValid() {
        return !txtWebsite.getText().isBlank() && !txtUsername.getText().isBlank();
    }

    private boolean isWebsiteValid() {
        return txtWebsite.getText().matches(FieldConstraints.WEBSITE_PATTERN);
    }

    private boolean isUsernameValid() {
        return txtUsername.getText().matches(FieldConstraints.VALID_USERNAME_PATTERN);
    }
}