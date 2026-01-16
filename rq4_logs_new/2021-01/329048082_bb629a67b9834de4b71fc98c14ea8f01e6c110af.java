package sample;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.ListView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;

import java.text.MessageFormat;
import java.util.Date;


public class Controller {

    @FXML
    private TextField inputField;

    @FXML
    private ListView<String> listView;

    @FXML
    private TextField nicknameField;

    private String nickname;

    @FXML
    private TextArea textArea;

    private final ObservableList<String>  nicknameList  = FXCollections.observableArrayList("user1", "user2", "user3");

    @FXML
    void initialize() {
        nickname = "Nickname";
        setNicknameField(nickname);
        listView.setItems(nicknameList);
    }

    private void setNicknameField(String nickname) {
        nicknameField.appendText(nickname);
    }

    @FXML
    void sendMessage() {
        String message = inputField.getText().trim();
        if (message.length() != 0){
            addMessageToTextArea(message);
        }
        else{
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Input Error");
            alert.setHeaderText("Ошибка ввода сообщения");
            alert.setContentText("Нельзя отправлять пустое сообщение");
            alert.show();
        }
        inputField.clear();
    }

    private void addMessageToTextArea(String message) {
        String result = MessageFormat.format("{0, date} {0, time, medium} [{1}] \n> " + message + "\n", new Date(), nickname);
        textArea.appendText(result);
    }

    @FXML
    void showAbout() {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("About");
        alert.setHeaderText("Chat Client");
        alert.setContentText("Клинтская программа сетеаого чата");
        alert.show();
    }

    @FXML
    void exit() {
        System.exit(0);
    }
}