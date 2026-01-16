package controller;

import model.user.User;
import model.user.UserRole;
import model.utils.Utils;
import model.utils.UtilsDatabase;
import view.LogInForm;

import javax.swing.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class LogInFormController {
    private static LogInForm logInForm;
    private static LogInFormController logInFormController;

    public LogInFormController() {
        control();
    }

    public static LogInFormController getInstance() {
        if (Objects.isNull(logInFormController)) {
            logInForm = new LogInForm();
            logInFormController = new LogInFormController();
        }
        init();
        return logInFormController;
    }

    public void control() {
        logInForm.getFrame().addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent windowEvent) {
                MainFormController.getInstance();
            }
        });
        logInForm.getButtonRegister().addActionListener(e -> {
            RegisterFormController.getInstance();
            logInForm.getFrame().dispose();
        });
        logInForm.getButtonLogIn().addActionListener(e -> onButtonLogIn());
        logInForm.getButtonBack().addActionListener(e -> onButtonBack());
        logInForm.getButtonRegister().addActionListener(e -> onButtonRegister());
    }

    private void onButtonLogIn() {
        try {
            if(UtilsDatabase.runSqlFunction("check_if_user_and_password_exist", List.of(logInForm.getTextFieldLogin().getText(), String.valueOf(logInForm.getPasswordFieldPassword().getPassword()))).next()){
                User user = getUser();
                if(user != null){
                    Utils.PARAMS.put("CURRENT_USER", user);
                    logInForm.getFrame().dispose();
                    Utils.displayOptionPane("Logged in successfully", "Success", JOptionPane.INFORMATION_MESSAGE);
                    MainFormUserController.getInstance();
                }
            } else
                Utils.displayOptionPane("Login and Password do not match", "Error", JOptionPane.ERROR_MESSAGE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        logInForm.getTextFieldLogin().setText("");
        logInForm.getPasswordFieldPassword().setText("");
    }

    private void onButtonRegister() {
    }

    private void onButtonBack() {
        if(Utils.PARAMS.containsKey("CURRENT_USER"))
            MainFormUserController.getInstance();
        else
            MainFormController.getInstance();
        logInForm.getFrame().dispose();
    }

    private static void init() {
        logInForm.getFrame().setVisible(true);
    }

    private static User getUser() throws SQLException {
        ResultSet userRS = UtilsDatabase.runSqlFunction("get_user_by_login", List.of(logInForm.getTextFieldLogin().getText()));

        if (userRS.next()) {
            int idUser = userRS.getInt("id_user");
            String login = userRS.getString("login");
            String password = userRS.getString("password");
            String role = userRS.getString("role");

            User user = new User(idUser, login, password, UserRole.valueOf(role));

            return user;
        } else {
            Utils.displayOptionPane("User not found", "Error", JOptionPane.ERROR_MESSAGE);
            return null;
        }
    }
}
