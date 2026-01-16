package com.example.cringecoding.Controllers;

import com.example.cringecoding.Models.Client;
import com.example.cringecoding.Models.Report;
import com.example.cringecoding.Services.OtherUtils;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;

import java.util.Date;

public class ReportDialogController {

    @FXML
    private TextField phoneNumberField;

    @FXML
    private TextArea complaintField;

    private final ClientService clientService = new ClientService();

    @FXML
    private void handleSubmit() {
        String phoneNumber = phoneNumberField.getText();
        String complaintText = complaintField.getText();

        if (phoneNumber.isEmpty() || complaintText.isEmpty()) {
            showAlert("Error", "Both fields must be filled out.");
            return;
        }

        Client client = clientService.getClientByPhoneNumber(phoneNumber);
        if (client == null) {
            showAlert("Error", "Client with this phone number does not exist.");
            return;
        }

        Report report = new Report();
        report.setClient(client);
        report.setDescription(complaintText);
        report.setReportDate(new Date());
        clientService.saveReport(report);

        showAlert("Success", "Report submitted successfully.");
    }

    private void showAlert(String title, String message) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle(title);
        alert.setContentText(message);
        alert.showAndWait();
    }

    @FXML
    private void handleBack(ActionEvent event) {
        OtherUtils.changeScene(event, "/com/example/cringecoding/client.fxml", "?");
    }
}