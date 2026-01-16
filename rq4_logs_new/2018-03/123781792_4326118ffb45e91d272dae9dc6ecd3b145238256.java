/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package warsztatsamochodowy.controllers;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.stage.Stage;

/**
 * FXML Controller class
 *
 * @author Administrator
 */
public class LoginController implements Initializable {

    @FXML
    private TextField username;

    @FXML
    private Button login;

    @FXML
    private PasswordField password;

    @FXML
    private void buttonLogowanie_click(ActionEvent event) {
        sprawdzLogowanie(username.getText(), password.getText());
    }

    private void error(String message) {

        Alert alert = new Alert(AlertType.ERROR);
        alert.setTitle("Wystąpił błąd");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

    private void zalogujUzytkownika(String stanowisko) {
        try {
            FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/warsztatsamochodowy/views/MainMenu.fxml"));
            Parent root1 = (Parent) fxmlLoader.load();

            MainMenuController mainmenu_controller = fxmlLoader.<MainMenuController>getController();
            mainmenu_controller.stanowisko = stanowisko;

            Stage mainmenu_scene = new Stage();
            mainmenu_scene.setScene(new Scene(root1));
            mainmenu_scene.setTitle("Warsztat samochodowy - Menu główne");
            mainmenu_scene.show();
            Stage login_scene = (Stage) login.getScene().getWindow();
            login_scene.close();
        } catch (IOException ex) {
            Logger.getLogger(LoginController.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void sprawdzLogowanie(String username, String password) {

        if (konta.containsKey(username)) {

            if (konta.get(username)[0].equals(password)) {
                zalogujUzytkownika(konta.get(username)[1]);
            } else {
                error("Wprowadzono złe hasło!");
            }
        } else {
            error("Użytkownik nie istnieje!");
        }

    }

    HashMap<String, String[]> konta = new HashMap<String, String[]>();

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        konta.put("Janusz", new String[]{"123456", "Kierownik"});
        konta.put("Grażyna", new String[]{"brajanek2010", "Recepcjonistka"});
        konta.put("Heniek", new String[]{"kochamgrazynke", "Mechanik"});
        konta.put("Tadeusz", new String[]{"qwerty", "Administrator"});

    }

}