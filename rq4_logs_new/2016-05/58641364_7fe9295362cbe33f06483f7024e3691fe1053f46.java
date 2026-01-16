/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fashionshoppen;

import java.net.URL;
import java.sql.Connection;
import java.util.ResourceBundle;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TextField;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.Pane;
import services.ServerRequest;
import source.Customer;
import source.Catalog;
import source.Webshop;

/**
 *
 * @author aleksander
 */
public class FXMLDocumentController implements Initializable
{

    private Webshop webshop;
    @FXML
    private ComboBox CBAcc;
    @FXML
    private ComboBox CBCloth;
    @FXML
    private ComboBox CBShoes;
    @FXML
    private Button BTNHclothes;
    @FXML
    private Button BTNDclothes;
    @FXML
    private Pane RegisterPane;
    @FXML
    private Button BTNRegister;
    @FXML
    private TextField LoginEmail;
    @FXML
    private TextField LoginPW;
    @FXML
    private Button BTNLogin;
    @FXML
    private Label LblLogin;
    @FXML
    private Label LblNewReg;
    @FXML
    private Tab TabMainpage;
    @FXML
    private Tab TabLogin;
    @FXML
    private TextField TFsearch;
    @FXML
    private TabPane MainTabPane;
    @FXML
    private TextField regFirstName;
    @FXML
    private TextField regLastName;
    @FXML
    private TextField regEmail;
    @FXML
    private TextField regPW1;
    @FXML
    private TextField regPW2;
    @FXML
    private Button searchBtn;

    @Override
    public void initialize(URL url, ResourceBundle rb)
    {
        webshop = new Webshop();
        CBCloth.setVisible(false);
        CBShoes.setVisible(false);
        CBAcc.setVisible(false);

    }

    @FXML
    private void showDClothes(ActionEvent event)
    {
        populateCBD();
        CBCloth.setVisible(true);
        CBCloth.setPromptText("Dametøj");
        CBShoes.setVisible(true);
        CBShoes.setPromptText("Damesko");
        CBAcc.setVisible(true);
        CBAcc.setPromptText("Accessories");
        BTNDclothes.setDisable(true);
        BTNHclothes.setDisable(false);
    }

    @FXML
    private void showHClothes(ActionEvent event)
    {
        CBCloth.setVisible(true);
        CBCloth.setPromptText("Herretøj");
        CBShoes.setVisible(true);
        CBShoes.setPromptText("Herresko");
        CBAcc.setVisible(true);
        CBAcc.setPromptText("Accessories");
        populateCBH();
        BTNHclothes.setDisable(true);
        BTNDclothes.setDisable(false);
    }

    private void populateCBH()
    {
        CBCloth.getItems().clear();
        CBShoes.getItems().clear();
        CBAcc.getItems().clear();

        CBCloth.getItems().addAll(
                "Nyheder", "Jakker og frakker", "T-shirts", "Jeans", "Skjorter", "Bukser", "Shorts",
                "Polo", "Badeshorts", "Trøjer", "Undertøj", "Strømper", "Jakkesæt");

        CBShoes.getItems().addAll(
                "Nyheder", "Hjemmesko", "Sandaler", "Sneakers", "Støvler");

        CBAcc.getItems().addAll(
                "Nyheder", "Tasker", "Handsker", "Punge", "Huer og kasketter", "Bælter", "Ure", "Butterfly",
                "Smykker", "Hatte", "Kortholder", "Halstørklæder");

    }

    private void populateCBD()
    {
        CBCloth.getItems().clear();
        CBShoes.getItems().clear();
        CBAcc.getItems().clear();

        CBCloth.getItems().addAll(
                "Nyheder", "Kjoler", "Bukser", "Jakker og frakker", "Nederdele", "Jeans", "Nattøj", "Badetøj", "Bluser",
                "Skjorter", "Toppe", "Shorts", "T-shirts", "Tunika", "Lingerie", "Strømpebukser", "Undertøj", "Heldragt");

        CBShoes.getItems().addAll(
                "Nyheder", "Støvler", "Sandaler", "Ballerina Sko", "Sneakers", "Gummistøvler", "Stiletter", "Støvletter",
                "Hjemmesko");

        CBAcc.getItems().addAll(
                "Nyheder", "Tasker", "Punge", "Smykker", "Bælter", "Tørklæder", "Ure",
                "Handsker", "Hatte", "Huer og kasketter", "Mobil- og tabletholder", "Solbriller");

    }

    @FXML
    private void handleLogin(MouseEvent event)
    {

        MainTabPane.getSelectionModel().select(1);
        RegisterPane.setVisible(false);

    }

    @FXML
    private void handleShowBasket(MouseEvent event)
    {
        MainTabPane.getSelectionModel().select(3);
    }

    @FXML
    private void handleLoginUser(ActionEvent event)
    {
        String email = LoginEmail.getText();
        String password = LoginPW.getText();
        Customer customer = new Customer(email, password);
        Customer returnedCustomer = customer.loginCustomer(customer);
        if (returnedCustomer == null)
        {
            System.out.println("Failed");
        }
        else
        {
            System.out.println("" + returnedCustomer.getAddress());
        }
    }

    @FXML
    private void handleRegister(ActionEvent event)
    {
        String firstName = regFirstName.getText();
        String lastName = regLastName.getText();
        String email = regEmail.getText();
        if (regPW1.getText().equals(regPW2.getText()))
        {
            String password = regPW1.getText();
            Customer customer = new Customer(firstName, lastName, email, password);
            customer.registerCustomer(customer);
        }
        else
        {

        }

        RegisterPane.setVisible(false);

    }

    @FXML
    private void handleCreateCustomer(MouseEvent event)
    {
        RegisterPane.setVisible(true);

    }

    @FXML
    private void handleSearch(ActionEvent event)
    {
        
        String st = TFsearch.getText();

        if (CBCloth.getValue() != null)
        {
            webshop.browseCategory(CBCloth.getValue().toString().toLowerCase(), st);

        }
        else
        {
            webshop.browseProductName(st);
        }

    }
}