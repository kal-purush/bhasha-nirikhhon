package ru.online;

import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;
import ru.online.utility.Utility;

import java.io.IOException;
import java.io.InputStream;

public class LoginWindow {

    private static final String LOGIN_SCENE_FXML = "/LoginScene.fxml";
    private static final String LOGIN_TITLE = "Login";
    private static final String PATH_TO_LOGIN_ICON = "/img/login_icon.png";

    private static Stage loginWindow;

    public static void displayLoginWindow(Stage primaryStage) throws IOException {

        int defScreenWidth = ClientApp.getDefaultScreenWidth();
        int defScreenHeight = ClientApp.getDefaultScreenHeight();

        loginWindow = primaryStage;
        loginWindow.setOnCloseRequest(e -> {
            e.consume();
            LoginWindow.closeProgram();
        });
        loginWindow.setTitle(LOGIN_TITLE);

        InputStream loginIconStream = MainWindow.class.getResourceAsStream(PATH_TO_LOGIN_ICON);
        Image chatIcon = new Image(loginIconStream);
        loginWindow.getIcons().add(chatIcon);

        FXMLLoader loginLoader = new FXMLLoader(MainWindow.class.getResource(LOGIN_SCENE_FXML));
        double windowWidth = defScreenWidth / 6.0;
        double windowHeight = defScreenHeight / 7.0;
        Scene loginScene = new Scene(loginLoader.load(), windowWidth, windowHeight);

        Utility.centerStage(loginWindow, windowWidth, windowHeight);
        loginWindow.setResizable(false);
        loginWindow.setScene(loginScene);
        loginWindow.show();
    }

    public static void closeProgram() {
        boolean answer = false;
        try {
            answer = ConfirmWindow.display();
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }
        if (answer) {
            Platform.exit();
        }
    }

    public static Stage getLoginWindow() {
        return loginWindow;
    }
}