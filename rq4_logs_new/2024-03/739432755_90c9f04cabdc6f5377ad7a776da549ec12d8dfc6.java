package application;

import java.io.IOException;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.stage.Stage;

public class MenuController {
	
	private Stage stage;
	private Scene scene;
	private Parent root;
	
	@FXML
	private Button VediClassifica;
	@FXML
	private Button CreaPartita;
	@FXML
	private Button CreaTorneo;
	@FXML
	private Button Logout;
	
	InfoPartita infopartita = new InfoPartita();
	
	//per creare una partita,il metodo preparazione prepara la lista delle partite salvate
	public void VaiaPrePartita(ActionEvent event) throws IOException {
		infopartita.Preparazione();
		FXMLLoader loader = new FXMLLoader(getClass().getResource("PrePartita.fxml"));
		root = loader.load();
		stage = (Stage)((Node)event.getSource()).getScene().getWindow();
		scene = new Scene(root);
		stage.setScene(scene);
		stage.show();
	}
	//per fare il logout e tornare alla pagina di login
	public void Logout(ActionEvent event) throws IOException {
		Parent root = FXMLLoader.load(getClass().getResource("Login.fxml"));
		stage = (Stage)((Node)event.getSource()).getScene().getWindow();
		scene = new Scene(root);
		stage.setScene(scene);
		stage.show();
	}
}