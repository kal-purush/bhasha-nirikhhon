package ui;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;


/**
 * Created by Samadoll on 2016-11-28.
 */
public class LoginUI {
    private JComboBox<String> jComboBox;
    private JTextField userText;
    private JPasswordField passwordField;
    private Boolean isClicked;
    private String aChoice;

    public LoginUI() {
        aChoice = "";
        isClicked = false;
        JFrame frame = new JFrame("Login");
        frame.setSize(350,200);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JPanel panel = new JPanel();
        frame.add(panel);
        paintComponents(panel);
        frame.setVisible(true);
    }

    private void paintComponents(JPanel panel) {
        panel.setLayout(null);
        onCreate(panel);
    }

    private void onCreate(JPanel panel) {

//        jComboBox = new JComboBox<>();
//        jComboBox.addItem("1.Login");
//        jComboBox.addItem("2.Register");
//        jComboBox.setBounds(10,10,165,25);
//        panel.add(jComboBox);

        JLabel usernameLabel = new JLabel("Username:");
        usernameLabel.setBounds(10,40,80,25);
        panel.add(usernameLabel);

        userText = new JTextField(20);
        userText.setBounds(100,40,165,25);
        panel.add(userText);

        JLabel passwordLabel = new JLabel("Password:");
        passwordLabel.setBounds(10,70,80,25);
        panel.add(passwordLabel);
        passwordField = new JPasswordField(20);
        passwordField.setBounds(100,70,165,25);
        panel.add(passwordField);

        JButton loginButton = new JButton("Login");
        loginButton.setBounds(50,100,80,25);
        loginButton.addActionListener(new LoginAction());
        panel.add(loginButton);

        JButton registerButton = new JButton("Register");
        registerButton.setBounds(150,100,80,25);
        registerButton.addActionListener(new RegisterAction());
        panel.add(registerButton);
    }

    public String getChoice() {
        System.out.println(aChoice);
        return aChoice;
    }

    public void setChoice(String choice) {
        this.aChoice = choice;
    }

    public String getUsername() {
        System.out.println(userText.getText());
        return userText.getText();
    }

    public String getPassword() {
        System.out.println(passwordField.getPassword());
        return passwordField.getSelectedText();
    }


//    public Boolean getIsClicked() {
//        return isClicked;
//    }

    private class LoginAction implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            setChoice("1");
            getChoice();
            getUsername();
            getPassword();
            isClicked = true;
        }
    }

    private class RegisterAction implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            setChoice("2");
            getChoice();
            getUsername();
            getPassword();
            isClicked = true;
        }
    }

    public static void main(String[] args) {
        new LoginUI();
    }

}