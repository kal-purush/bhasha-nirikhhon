package screens;

import LoginUseCase.LoginPresenter;
import LoginUseCase.LoginResponse;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class LoggedInScreen extends JPanel implements ActionListener {

    public LoggedInScreen(){
        JLabel title = new JLabel("Got It");
        title.setAlignmentX(Component.CENTER_ALIGNMENT);
    }

    public LoggedInScreen(LoginResponse response) {
        JLabel title = new JLabel("Got It");
        JLabel username = new JLabel(response.getEmail());
        JLabel loggedIn = new JLabel("Logged In");
        this.add(title);
        this.add(loggedIn);
        this.add(username);
    }


    @Override
    public void actionPerformed(ActionEvent e) {
        System.out.println("click");
    }
}