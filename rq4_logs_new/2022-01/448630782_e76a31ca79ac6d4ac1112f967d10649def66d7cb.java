package net.htlgkr.groupK.chess;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.PasswordField;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TextField;
import javafx.scene.text.Text;
import javafx.stage.Stage;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

public class CNT_loginPrompt implements Initializable {

    @FXML
    public SplitPane loginPrompt_splitpane;

    @FXML
    public Text text_createGame_incorrectData;

    @FXML
    public Text text_createGame_userName;

    @FXML
    public TextField textField_createGame_userName;

    @FXML
    public Text text_createGame_portNumber;

    @FXML
    public TextField textField_createGame_portNumber;

    @FXML
    public Text text_createGame_password;

    @FXML
    public PasswordField textField_createGame_password;

    @FXML
    public Button btn_createGame_sendRequest;

    @FXML
    public Text text_joinGame_incorrectData;

    @FXML
    public Text text_joinGame_userName;

    @FXML
    public TextField textField_joinGame_userName;

    @FXML
    public Text text_joinGame_ipAddress;

    @FXML
    public TextField textField_joinGame_ipAddress;

    @FXML
    public Text text_joinGame_portNumber;

    @FXML
    public TextField textField_joinGame_portNumber;

    @FXML
    public Text text_joinGame_password;

    @FXML
    public PasswordField textField_joinGame_password;

    @FXML
    public Button btn_joinGame_sendRequest;

    public SplitPane getLoginPrompt_splitpane() {
        return loginPrompt_splitpane;
    }

    public Text getText_createGame_incorrectData() {
        return text_createGame_incorrectData;
    }

    public Text getText_createGame_userName() {
        return text_createGame_userName;
    }

    public TextField getTextField_createGame_userName() {
        return textField_createGame_userName;
    }

    public Text getText_createGame_portNumber() {
        return text_createGame_portNumber;
    }

    public TextField getTextField_createGame_portNumber() {
        return textField_createGame_portNumber;
    }

    public Text getText_createGame_password() {
        return text_createGame_password;
    }

    public PasswordField getTextField_createGame_password() {
        return textField_createGame_password;
    }

    public Button getBtn_createGame_sendRequest() {
        return btn_createGame_sendRequest;
    }

    public Text getText_joinGame_incorrectData() {
        return text_joinGame_incorrectData;
    }

    public Text getText_joinGame_userName() {
        return text_joinGame_userName;
    }

    public TextField getTextField_joinGame_userName() {
        return textField_joinGame_userName;
    }

    public Text getText_joinGame_ipAddress() {
        return text_joinGame_ipAddress;
    }

    public TextField getTextField_joinGame_ipAddress() {
        return textField_joinGame_ipAddress;
    }

    public Text getText_joinGame_portNumber() {
        return text_joinGame_portNumber;
    }

    public TextField getTextField_joinGame_portNumber() {
        return textField_joinGame_portNumber;
    }

    public Text getText_joinGame_password() {
        return text_joinGame_password;
    }

    public PasswordField getTextField_joinGame_password() {
        return textField_joinGame_password;
    }

    public Button getBtn_joinGame_sendRequest() {
        return btn_joinGame_sendRequest;
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        Platform.runLater(() -> getLoginPrompt_splitpane().lookup(".split-pane-divider").setDisable(true));

        this.getBtn_createGame_sendRequest().setOnAction(actionEvent -> {
            System.out.println("new server created");
            Server server = new Server(this);
        });

        this.getBtn_joinGame_sendRequest().setOnAction(actionEvent -> {
            System.out.println("new client created");
            Client client = new Client(this);
        });
    }
}