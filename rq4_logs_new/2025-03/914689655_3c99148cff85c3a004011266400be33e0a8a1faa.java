package ui;

import model.AuthData;

import java.util.Objects;
import java.util.UUID;

public class ClientEval {
    private String input;
    private UUID authorization;
    public ClientEval(String input, UUID authorization){
        this.input = input;
        this.authorization = authorization;
    }

    public void eval(){
        if(Objects.equals(input, "help") || Objects.equals(input, "h")){
            helpEval();
        }
        if(Objects.equals(input, "logout")){
            logoutEval();
        }
        if(Objects.equals(input, "login")){
            loginEval();
        }
        if(Objects.equals(input, "register")){
            registerEval();
        }


    }
    public AuthData registerEval(){
        return null;
    }
    public AuthData loginEval(){
        return null;
    }

    public void logoutEval(){

    }

    public void helpEval(){
        new ClientUI(authorization).helpDisplay();
    }
}