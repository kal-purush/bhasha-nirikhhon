package application;

import java.net.URL;
import java.util.Optional;
import java.util.ResourceBundle;

import javafx.application.Platform;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

public class AnaSayfaController {

    @FXML
    private ResourceBundle resources;

    @FXML
    private URL location;

    @FXML
    private AnchorPane anchor_anaSayfa;

    @FXML
    private AnchorPane anchor_center;

    @FXML
    private AnchorPane anchor_lower;

    @FXML
    private AnchorPane anchor_top;

    @FXML
    private Button ayarlar;

    @FXML
    private BorderPane border;

    @FXML
    private Button calisan;

    @FXML
    private Button cikis_btn;

    @FXML
    private Button depo;

    @FXML
    private Button fatura;

    @FXML
    private ImageView image_icon;

    @FXML
    private Label lbl;

    @FXML
    private Button urunler_btn;

    @FXML
    private VBox vbox;

    @FXML
    void ayarlar_click(ActionEvent event) {

    }

    @FXML
    void calisanlar_click(ActionEvent event) {
    	try {
    		Stage stage=new Stage();
        	AnchorPane pane1=(AnchorPane) FXMLLoader.load(getClass().getResource("calisanlarListesi.fxml"));
        	Scene scene1=new Scene(pane1);
        	stage.setScene(scene1);
        	stage.setTitle("�al��anlar Listesi");
            stage.show();
			
		} catch (Exception e) {
			// TODO: handle exception
		}

    }

    @FXML
    void cikis_click(ActionEvent event) {
    	Alert alert=new Alert(AlertType.CONFIRMATION);
    	alert.setTitle("K�rtasiye Otomasyonu");
    	alert.setHeaderText("");
    	alert.setContentText("��kmak istedi�inizden emin misiniz");
    	
    	ButtonType btn1=new ButtonType("Evet");
    	ButtonType btn2=new ButtonType("Hay�r");
    	ButtonType btn3=new ButtonType("iptal", ButtonData.CANCEL_CLOSE);
    	alert.getButtonTypes().setAll(btn1, btn2, btn3);
    	Optional<ButtonType> sonuc= alert.showAndWait();
    	
    	if(sonuc.get()==btn1) {
    		System.out.println("Otomasyondan ��kt�n�z");
            Platform.exit();

    		
    	}
    	else if(sonuc.get()==btn2) {
    		System.out.println(" hay�r butonuna t�kland�..");
    	}
    	else if(sonuc.get()==btn3) {
    		System.out.println("iptal butonuna t�kland�..");
    	}
    	else {
    		System.out.println("�ptal tu�u");
    	}

    }

    @FXML
    void depo_click(ActionEvent event) {

    }

    @FXML
    void faturalar_click(ActionEvent event) {
    	try {
    		Stage stage=new Stage();
        	AnchorPane pane1=(AnchorPane) FXMLLoader.load(getClass().getResource("faturalar.fxml"));
        	Scene scene1=new Scene(pane1);
        	stage.setScene(scene1);
        	stage.setTitle("Faturalar");

            stage.show();
			
		} catch (Exception e) {
			// TODO: handle exception
		}

    }

    @FXML
    void urunler_click(ActionEvent event) {
    	try {
    		Stage stage=new Stage();
        	AnchorPane pane1=(AnchorPane) FXMLLoader.load(getClass().getResource("urunler.fxml"));
        	Scene scene1=new Scene(pane1);
        	stage.setScene(scene1);
        	stage.setTitle("�r�nler Listesi");

            stage.show();
			
		} catch (Exception e) {
			// TODO: handle exception
		}

    }

    @FXML
    void initialize() {
       
    }

}