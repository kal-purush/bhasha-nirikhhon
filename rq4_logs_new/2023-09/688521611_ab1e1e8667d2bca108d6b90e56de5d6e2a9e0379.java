package com.example.frontendjavafx.controller;

import com.example.frontendjavafx.security.AppUser;
import com.example.frontendjavafx.security.AppUserRole;
import com.example.frontendjavafx.service.RegistrationService;
import com.example.frontendjavafx.service.SceneSwitchService;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.CheckBox;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;

import java.io.IOException;
import java.util.ArrayList;

public class RegistrationController {

    @FXML
    private TextField userNameTextField;
    @FXML
    private PasswordField passwordField;
    @FXML
    private TextField apiTextField;
    @FXML
    private TextField apiKeyTextField;
    @FXML
    private CheckBox virtualAgentCheckBox;

    @FXML
    private void onClick_registration(ActionEvent event) throws IOException {

        String userName = userNameTextField.getText();
        String password = passwordField.getText();
        String api = apiTextField.getText();
        String apiKey = apiKeyTextField.getText();
        boolean isVirtualAgent = virtualAgentCheckBox.isSelected();

        AppUser registeringUser = new AppUser(
                null,
                userName,
                password,
                AppUserRole.USER,
                new ArrayList<>(),
                isVirtualAgent,
                api,
                apiKey
        );

        RegistrationService.getInstance().createNewAppUser(registeringUser);
        SceneSwitchService.getInstance().switchToLoginView(event);

    }

}