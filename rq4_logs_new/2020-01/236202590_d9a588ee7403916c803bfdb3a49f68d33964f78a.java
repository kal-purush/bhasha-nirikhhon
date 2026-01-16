package Controller_File;


import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextArea;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Stage;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ResourceBundle;

public class Controller implements Initializable {

    public static final String USER_FIELD = "user_field";
    public static final String TABLE_NAME = "log_table";
    public static final String TABLE_ID = "id";
    public static final String TABLE_HASLO = "haslo_field";

    public static final String SQL_NAME = "org.sqlite.JDBC";

    public static final String CONNECTION = "jdbc:sqlite:/home/tomasz/Pulpit/Projekt/src/LoginDatabase.db";

    public static final String LOGIN_PATH = "SELECT * FROM log_table WHERE user_field = ? and haslo_field = ?";

    public static final String STUDENT_FXML = "/FXML_File/Student.fxml";

    @FXML
    private AnchorPane mainAnchorPane;

    @FXML
    private URL location;

    @FXML
    private TextArea loginTextField;

    @FXML
    private PasswordField hasloTextField;

    @FXML
    private Button loginButton;

    @FXML
    private Label timeLabel;

    @FXML
    public void login_button_action(MouseEvent mouseEvent) {

        try {
            Class.forName(SQL_NAME);

            Connection conn = DriverManager.getConnection(CONNECTION);

            PreparedStatement ps = conn.prepareStatement(LOGIN_PATH);
            ps.setString(1, loginTextField.getText());
            ps.setString(2, hasloTextField.getText());

            ResultSet resultSet = ps.executeQuery();

            if (resultSet.next()) {

                System.out.println("Jestes zalogowany");
                LoadStage(STUDENT_FXML);
                ps.close();
                ((Node) (mouseEvent.getSource())).getScene().getWindow().hide();

            } else {
                System.out.println("Nie jestes zalogowany. Bledne haslo / login");
            }
        } catch (Exception e) {
            System.out.println("cos jest nie tak");
            e.printStackTrace();
        }
    }

    private void LoadStage(String fxml) {
        try {
            Parent root = FXMLLoader.load(getClass().getResource(fxml));
            Stage stage = new Stage();
            stage.setScene(new Scene(root));
            stage.show();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Problem z zaladowanie fxmla");
        }
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

    }


}

