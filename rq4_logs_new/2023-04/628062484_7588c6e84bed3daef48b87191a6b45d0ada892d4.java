package test.hallohallo;


import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.stage.Stage;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;

public class EditTourController {
    @FXML
    private ComboBox<String> typeComboBox;

    @FXML
    private TextField fxTourName;
    @FXML
    private TextArea fxTourDescription;
    @FXML
    private TextField fxPrice;
    @FXML
    private TextField fxPostcode;
    @FXML
    private TextField fxAddress;
    @FXML
    private TextField fxCountry;
    @FXML
    private TextField fxAvailableSpots;
    @FXML
    private DatePicker fxDate;
    @FXML
    private TextField fxTime;

    @FXML
    private Button fxBackButton;
    @FXML
    private TextField fxCity;
    @FXML
    private Label fxFilled;
    @FXML
    private Label fxTourID;
    @FXML
    private Label fxHostName;

    public void setUserData(String username, String TourID) {
        fxHostName.setText(username);
        fxTourID.setText(TourID);
        populateTourData(TourID);
    }

  @FXML

    private Button fxEditTourButton;

    @FXML
    private void initialize() {
        // Initialize the typeComboBox
        typeComboBox.getItems().addAll("Activity", "Romantic");
    }

    @FXML
    private void handleBackButtonClick() throws IOException {
        Stage stage = (Stage) fxBackButton.getScene().getWindow();
        Scene scene = stage.getScene();
        FXMLLoader loader = new FXMLLoader(getClass().getResource("TourView.fxml"));
        Parent root = loader.load();
        TourView Controller = loader.getController();
        Controller.setUserData(fxHostName.getText(), fxTourID.getText());
        scene.setRoot(root);
        stage.setTitle("Tour viewer");
    }
    @FXML
    private void handleEditButtonAction() {
        try {
            Connection connection = DriverManager.getConnection(YourLocation.Command());
            String updateTour = "UPDATE Tour SET title=?, description=?, price=?, type=?, country=?, address=?, post_code=?, city=?, date=?, time=?, available_slots=? WHERE id=?";
            PreparedStatement preparedStatement = connection.prepareStatement(updateTour);

            preparedStatement.setString(1, fxTourName.getText());
            preparedStatement.setString(2, fxTourDescription.getText());
            preparedStatement.setDouble(3, Double.parseDouble(fxPrice.getText()));
            preparedStatement.setString(4, typeComboBox.getValue());
            preparedStatement.setString(5, fxCountry.getText());
            preparedStatement.setString(6, fxAddress.getText());
            preparedStatement.setString(7, fxPostcode.getText());
            preparedStatement.setString(8, fxCity.getText());
            preparedStatement.setString(9, fxDate.getValue().toString());
            preparedStatement.setString(10, fxTime.getText());
            preparedStatement.setInt(11, Integer.parseInt(fxAvailableSpots.getText()));
            preparedStatement.setInt(12, Integer.parseInt(fxTourID.getText()));
            preparedStatement.executeUpdate();
            preparedStatement.close();
            connection.close();
            handleBackButtonClick();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void populateTourData(String tourID) {
        try {
            Connection connection = DriverManager.getConnection(YourLocation.Command());
            String queryTour = "SELECT * FROM Tour WHERE id = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(queryTour);
            preparedStatement.setInt(1, Integer.parseInt(tourID));
            ResultSet resultSetTour = preparedStatement.executeQuery();

            if (resultSetTour.next()) {
                fxTourName.setText(resultSetTour.getString("title"));
                fxTourDescription.setText(resultSetTour.getString("description"));
                fxPrice.setText(String.valueOf(resultSetTour.getDouble("price")));
                typeComboBox.setValue(resultSetTour.getString("type"));
                fxCountry.setText(resultSetTour.getString("country"));
                fxAddress.setText(resultSetTour.getString("address"));
                fxPostcode.setText(resultSetTour.getString("post_code"));
                fxCity.setText(resultSetTour.getString("city"));
                fxDate.setValue(LocalDate.parse(resultSetTour.getString("date")));
                fxTime.setText(resultSetTour.getString("time"));
                fxAvailableSpots.setText(String.valueOf(resultSetTour.getInt("available_slots")));
            }

            resultSetTour.close();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

