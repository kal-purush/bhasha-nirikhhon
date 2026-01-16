package es.antoniodominguez.jarronesarena;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;

public class App extends Application {
    
    BorderPane paneRoot;
    @Override
    public void start(Stage stage) {
        short tamXPantalla = 640;
        short tamYPantalla = 480;
        
        paneRoot = new BorderPane();
        var scene = new Scene(paneRoot, tamXPantalla, tamYPantalla);
        stage.setScene(scene);
        stage.show();
        
        Logica logica = new Logica();

        logica.colocarNumAleatorios();
        logica.mostrarJarronesConsola();
        logica.cambiarColumna();
        
        Tablero tablero = new Tablero(logica);
        paneRoot.setCenter(tablero);      
        logica.mostrarJarronesConsola();
        PanelSuperior panelSuperior = new PanelSuperior (logica, tablero);
        paneRoot.setTop(panelSuperior);
    }

    public static void main(String[] args) {
        launch();
    }

}