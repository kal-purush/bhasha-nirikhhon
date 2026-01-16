package com.artpro.task2;

import com.artpro.task2.service.AuthService;
import com.artpro.task2.service.AuthServiceImpl;

import java.util.List;

public class Task2 {
    public static void main(String[] args) {
        List<String[]> listForBrute = List.of(
                new String[]{"login", "psw", "confPsw"},
                new String[]{"", "", ""}
                //тут у нас будут кейсы
        );
        AuthService service = new AuthServiceImpl();
        for (String[] check : listForBrute) {
            System.out.println("Login: " + check[0] + "; Password: " + check[1] + "; Confirmation: " + check[2] + ";");
            // [0] = login [1] = psw;
            try {
                System.out.println(service.checkAuthorization(check[0], check[1], check[2]) ? "Code 200" : "Code 404");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
