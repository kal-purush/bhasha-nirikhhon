package adduser;

import com.jfoenix.controls.JFXButton;
import com.jfoenix.controls.JFXPasswordField;
import com.jfoenix.controls.JFXTextField;
import com.jfoenix.controls.JFXToggleButton;
import static homepage.HomePageController.decorator1;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ResourceBundle;
import javafx.event.ActionEvent;
import javafx.event.Event;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.stage.Stage;
import static project.Constants.BASE_URL;
import project.DataBase;
import project.Paths;
import project.User;


public class AddUserController implements Initializable {

    @FXML
    private JFXTextField UserNameField;
    @FXML
    private JFXPasswordField PasswordField;
    @FXML
    private JFXButton ButtonLogin;
    @FXML
    private ImageView key_pic_Login_Btn;
    @FXML
    private JFXToggleButton isAdminButton;

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        
    }

    @FXML
    private void NewUserAction(Event event) {
        try {
            String apiUrl = BASE_URL + "/save-user";
            URL uri = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) uri.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);
            
            System.out.println("API Called for saving user having URL: " + apiUrl);
            if ("".equals(UserNameField.getText()) || "".equals(PasswordField.getText())) {
                Alert alert = new Alert(Alert.AlertType.WARNING, "", ButtonType.OK);
                alert.setHeaderText("Please fill all the fields");
                alert.setTitle("Error");
                ((Stage) alert.getDialogPane().getScene().getWindow()).getIcons().add(new Image("/img/Error01.png"));
                alert.showAndWait();
                return;
            }

            System.out.println("1");
            boolean AdminStatus = isAdminButton.isSelected();
            System.out.println("2");
            User user = new User(UserNameField.getText(), PasswordField.getText(), AdminStatus);
            
            String requestBody = user.toString();

            // Write the JSON data to the request body
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = requestBody.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            System.out.println("Response Code for saving user: " + responseCode);
            
            System.out.println("3");
            boolean SaveUser = DataBase.SaveUser(user);
            System.out.println("4");
            if (!SaveUser) {
                System.out.println("5");
                Alert alert = new Alert(Alert.AlertType.CONFIRMATION, "", ButtonType.OK);
                alert.setHeaderText("The username is already taken. Please try another one.");
                alert.setTitle("Error");
                System.out.println("6");
                ((Stage) alert.getDialogPane().getScene().getWindow()).getIcons().add(new Image("/img/Error01.png"));
                System.out.println("7");
                alert.showAndWait();
                System.out.println("8");
                return;
            }
            System.out.println("9");
            PasswordField.setText("");
            UserNameField.setText("");
            Alert alert = new Alert(Alert.AlertType.INFORMATION, "", ButtonType.OK);
            alert.setHeaderText("User added successfully");
            alert.setTitle("Notification");
            alert.showAndWait();
            System.out.println("10");
            
            // Close the connection
            connection.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @FXML
    private void isAdminButtonAction(ActionEvent event) {
        if (isAdminButton.isSelected()) {
            isAdminButton.setText("Admin");
        } else {
            isAdminButton.setText("Receptionist");
        }
    }

    @FXML
    public void handleUsers(Event event) {

        System.out.println("Users label clicked");
        try {
            Parent root = FXMLLoader.load(getClass().getResource(Paths.USERSVIEW));
            decorator1.setContent(root);
            decorator1.setTitle("Users");
            root.requestFocus();
        } catch (Exception ex) {
            System.out.println("Error load Users FXML !");
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
    }
}