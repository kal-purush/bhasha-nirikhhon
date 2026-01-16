/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package projekt;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.stage.Stage;

/**
 * FXML Controller class
 *
 * @author Maciek
 */
public class Dodaj_projektController implements Initializable {
    @FXML
    private Button b_dodaj;
    @FXML
    private Button b_anuluj;
    @FXML
    private TextField f_nazwa;
    @FXML
    private TextField f_poczatek;
    @FXML
    private TextField f_koniec;
    @FXML
    private TextArea f_opis;
    @FXML
    private TableView<User> tv_vip;
    @FXML
    private TableColumn<User, String> tc_nazwisko;
    @FXML
    private TableColumn<User, String> tc_imie;
    @FXML
    private TableView<User> tv_pracownicy;
    @FXML
    private TableColumn<User, String> tc_nazwisko1;
    @FXML
    private TableColumn<User, String> tc_imie1;
    @FXML
    private Button b_wybierz;

    /**
     * Initializes the controller class.
     */
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        try {
            final ObservableList <User> data = FXCollections.observableArrayList();
            final ObservableList <User> data1 = FXCollections.observableArrayList();
            tc_nazwisko.setCellValueFactory(new PropertyValueFactory<User,String>("Nazwisko"));
            tc_imie.setCellValueFactory(new PropertyValueFactory<User,String>("Imie"));
            
            tc_nazwisko1.setCellValueFactory(new PropertyValueFactory<User,String>("Nazwisko"));
            tc_imie1.setCellValueFactory(new PropertyValueFactory<User,String>("Imie"));
             //tu trzeba zmienic zapytania
            Class.forName("com.mysql.jdbc.Driver");
            Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/pz","root","");
            PreparedStatement statment = con.prepareStatement("select Nazwisko , Imie from uzytkownicy");
            ResultSet result = statment.executeQuery();
            
            while(result.next()){
                data.add(new User(
                        result.getString("Nazwisko"),
                        result.getString("Imie")
                ));
                tv_vip.setItems(data);
            }
            
            //test
            //tu trzeba zmienic zapytania
            statment = con.prepareStatement("select Nazwisko , Imie from uzytkownicy where Nazwisko like 'B%'");
            result = statment.executeQuery();
            
            while(result.next()){
                data1.add(new User(
                        result.getString("Nazwisko"),
                        result.getString("Imie")
                ));
                tv_pracownicy.setItems(data1);
            }
            
            b_anuluj.setOnAction(new EventHandler<ActionEvent>() {
                @Override
                public void handle(ActionEvent actionEvent) {
                    
                    try {
                        
                        Parent projekty_parent = FXMLLoader.load(getClass().getResource("Projekty.fxml"));
                        Scene projekty_scene = new Scene(projekty_parent);
                        Stage projekty_stage = (Stage) ((Node) actionEvent.getSource()).getScene().getWindow();
                        projekty_stage.setScene(projekty_scene);
                        projekty_stage.show();
                        
                    } catch (IOException ex) {
                        Logger.getLogger(mainController.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    
                }
            });   
        
        b_dodaj.setOnAction(new EventHandler<ActionEvent>() {
                @Override
                public void handle(ActionEvent actionEvent) {
                    
                    try {
                        Class.forName("com.mysql.jdbc.Driver");
Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/pz","root","");
PreparedStatement statment = con.prepareStatement("insert into projekty (Nazwa,Opis,Poczatek,Koniec) VALUES ('"+f_nazwa.getText()+"','"+f_opis.getText()+"','"+f_poczatek.getText()+"','"+f_koniec.getText()+"')");
statment.executeUpdate();
//trzeba poprawic komende sql


                        
                        Parent projekty_parent = FXMLLoader.load(getClass().getResource("Projekty.fxml"));
                        Scene projekty_scene = new Scene(projekty_parent);
                        Stage projekty_stage = (Stage) ((Node) actionEvent.getSource()).getScene().getWindow();
                        projekty_stage.setScene(projekty_scene);
                        projekty_stage.show();
                        
                    } catch (IOException ex) {
                        Logger.getLogger(mainController.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (ClassNotFoundException ex) {
                        Logger.getLogger(Dodaj_projektController.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (SQLException ex) {
                        Logger.getLogger(Dodaj_projektController.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    
                }
            });   
        
        
        
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Dodaj_projektController.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Dodaj_projektController.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }    
    
}