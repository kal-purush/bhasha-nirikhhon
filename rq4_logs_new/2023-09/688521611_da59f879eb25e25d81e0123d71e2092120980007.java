package com.example.frontendjavafx.controller;

import com.example.frontendjavafx.security.AuthenticationService;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.ComboBox;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class LoginController {

    @FXML
    private TextField userName_TF;

    @FXML
    private PasswordField password_TF;

    private String STUDENTS_URL_BACKEND = System.getenv("BACKEND_STUDENT_URI");

    public void initialize() {
    }

    /*@FXML
    public void onClick_switchToRegisterView(ActionEvent event) throws IOException {
        SceneSwitchService.getInstance().switchToRegistrationView(event);
    }*/

    @FXML
    public void onClick_login(ActionEvent event) throws IOException {
        String loginName = userName_TF.getText();
        String password = password_TF.getText();
        boolean res = AuthenticationService.getInstance().login(loginName, password);

        if (res &&
                ! AuthenticationService.getInstance().getUsername().equals("anonymousUser")
        )
        {
            System.out.println("LOGIN NAME IS _______________ " + AuthenticationService.getInstance().getUsername());

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(STUDENTS_URL_BACKEND + "/api/auth/" + loginName))
                    .header("Accept", "application/json")
                    .header("Cookie", "JSESSIONID=" + AuthenticationService.getInstance().getSessionId())
                    .build();

            var response = AuthenticationService.getInstance().getClient().sendAsync(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.join().statusCode();
            String body = response.join().body();

            if (statusCode==200 && response.join().body().length() > 0) {
                //SceneSwitchService.getInstance().switchToStudentView(event);
                System.out.println("Hi! " + response.join().body());
                System.out.println("LOGIN SUCCESSFUL! Will jump to chat view! --> Newflash: won't! ");
            } else {
                System.out.println("LOGIN FAILED! PROBABLY ROLE INCORRECT OR INTERNAL SERVER ERROR");
                System.out.println("Status Code: " + statusCode);
                System.out.println("Response: " + body);
            }
        }
        else {
            System.out.println("LOGIN NAME IS _______________ " + AuthenticationService.getInstance().getUsername());
            System.out.println("LOGIN FAILED! USER NAME OR LOGIN INCORRECT OR INTERNAL SERVER ERROR");
        }
    }



}