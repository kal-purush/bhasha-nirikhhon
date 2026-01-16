package controller;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.ListView;
import javafx.scene.input.MouseEvent;
import javafx.stage.Stage;
import model.Hibernate.Tables.Food;
import model.Hibernate.Tables.User_Possess;
import model.Hibernate.Tables.Users;

import javax.persistence.criteria.CriteriaBuilder;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

public class ControllerAdmin implements Initializable {

    private Stage stage;
    private Scene scene;

    private User_Possess deleted_food;


    @FXML
    private ListView<String> listOfUsers;

    @FXML
    private ListView<Food> listOfFood;

    @FXML
    private ListView<?> listOfStockage;


    @FXML
    void getFoodtoDelete(MouseEvent event) {
       // this.deleted_food = DisplayListofood.getSelectionModel().getSelectedItem();

    }

    @FXML
    void DeleteAFood(javafx.event.ActionEvent event) {

        Main.session.utilities.removeFoodUserPossess(deleted_food.getId_food(),deleted_food.getId_storage(),deleted_food.getFood_add_date());
       // refresh();
    }

    @FXML
    void SelectedFood(MouseEvent event) {

    }

    @FXML
    void SelectedStockage(MouseEvent event) {

    }

    @FXML
    void handleCloseButtonAction(ActionEvent event) {

    }

    @FXML
    void selectedUser(MouseEvent event) {

    }



    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        ObservableList<String> users = FXCollections.observableArrayList(Main.session.utilities.getAllUsername());
        this.listOfUsers.setItems(users);

        ObservableList<Food> foods = FXCollections.observableArrayList(Main.session.utilities.getFoodOfTableFood());
        this.listOfFood.setItems(foods);

    }


    public void handleCloseButtonAction(javafx.event.ActionEvent actionEvent) throws IOException {
        Parent main_page = FXMLLoader.load(getClass().getResource("/homepage2.fxml"));
        stage = (Stage) ((Node)actionEvent.getSource()).getScene().getWindow();
        scene = new Scene(main_page);
        stage.setScene(scene);
        stage.show();
    }
}