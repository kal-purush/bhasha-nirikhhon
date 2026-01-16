/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package warsztatsamochodowy;

import java.io.IOException;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

/**
 *
 * @author lukasz
 */
public class Helper {
    public void powrotDoMenu() throws IOException{
           FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/warsztatsamochodowy/views/MainMenu.fxml"));
                    Stage mainMenu = new Stage();
                    Parent root = (Parent) fxmlLoader.load();
                    mainMenu.setScene(new Scene(root));
                    mainMenu.show();
                    mainMenu.setTitle("Warsztat samochodowy - Menu główne");

    }
    public void sceneSwitcher(String SciezkaFXML, String tytul) throws IOException{
           FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource(SciezkaFXML));
                    Stage settings_scene = new Stage();
                    Parent root = (Parent) fxmlLoader.load();
                    settings_scene.setScene(new Scene(root));
                    settings_scene.setTitle(tytul);
                    settings_scene.show();
    }
}