package br.fipp.sisdentalfx;

import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.input.MouseEvent;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

public class AdmViewController implements Initializable {
    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

    }

    public void onRelatorios(MouseEvent mouseEvent) {
    }

    public void onCadastros(MouseEvent mouseEvent) throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(HelloApplication.class.getResource("agendamento-view.fxml"));
        fxmlLoader.load();
        HelloController.staticpainel.setCenter(fxmlLoader.getRoot());
    }
}