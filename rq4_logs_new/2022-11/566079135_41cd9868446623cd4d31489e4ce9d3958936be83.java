package screens;

import LoginUseCase.LoginPresenter;
import LoginUseCase.LoginResponse;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class LoginFailed extends JPanel implements ActionListener, Screen{

    public LoginFailed() {
        JLabel title = new JLabel("Login Failed");
        JLabel loggedIn = new JLabel("Not Logged In");
        JButton back = new JButton("back");
        this.add(title);
        this.add(loggedIn);
        this.add(back);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        System.out.println("click");
    }
}