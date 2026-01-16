package ru.moore;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.stage.Modality;
import javafx.stage.Stage;

import ru.moore.controller.ActiveController;
import ru.moore.controller.AuthDialogController;

import java.util.List;

public class Main extends Application {

    public static final List<String> USERS_TEST_DATA = List.of("Oleg", "Alexey", "Peter");

    private Stage primaryStage;
    private Stage authDialogStage;
    private Network network;
    private ActiveController activeController;
    private AuthDialogController authController;

    @Override
    public void start(Stage primaryStage) throws Exception {
        this.primaryStage = primaryStage;
        FXMLLoader authLoader = new FXMLLoader();

        authLoader.setLocation(Main.class.getResource("view/authDialog.fxml"));
        Parent authDialogPanel = authLoader.load();
        authDialogStage = new Stage();

        authDialogStage.setTitle("Аутентификая чата");
        authDialogStage.initModality(Modality.WINDOW_MODAL);
        authDialogStage.initOwner(primaryStage);
        Scene scene = new Scene(authDialogPanel);
        authDialogStage.setScene(scene);
        authDialogStage.show();

        network = new Network();
        if (!network.connect()) {
            showNetworkError("", "Нет соединения с сервером");
        }

        authController = authLoader.getController();
        authController.setNetwork(network);
        authController.setClientApp(this);

        FXMLLoader mainLoader = new FXMLLoader();
        mainLoader.setLocation(Main.class.getResource("view/activeForm.fxml"));

        Parent root = mainLoader.load();

        primaryStage.setTitle("Чат");
        primaryStage.setScene(new Scene(root, 600, 400));

        activeController = mainLoader.getController();
        activeController.setNetwork(network);

        primaryStage.setOnCloseRequest(event -> {
            network.close();
        });

        network.timerMessages(authController);
    }

    public static void showNetworkError(String errorDetails, String errorTitle) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Ошибка сети");
        alert.setHeaderText(errorTitle);
        alert.setContentText(errorDetails);
        alert.showAndWait();
    }

    public static void main(String[] args) {
        launch(args);
    }

    public void openChat() {
        authDialogStage.close();
        primaryStage.show();
        primaryStage.setTitle(network.getUsername());
        network.waitMessages(activeController);
    }
}