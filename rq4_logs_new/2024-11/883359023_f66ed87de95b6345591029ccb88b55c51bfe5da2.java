package view;

import interface_adapter.ForgotPassword.ForgotPasswordViewModel;
import interface_adapter.ForgotPassword.ForgotPasswordState;

import javax.swing.*;
import java.awt.*;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

public class ForgotPasswordView extends JPanel implements PropertyChangeListener {

    private final String viewName = "Forgot Password";
    private final ForgotPasswordViewModel forgotPasswordViewModel;

    private final JLabel securityquestion;

    private final JTextField usernameInputField = new JTextField(15);
    private final JTextField securityanswerInputField = new JTextField(15);
    private final JTextField passwordInputField = new JTextField(15);
    private final JButton setPassword;

    public ForgotPasswordView(ForgotPasswordViewModel forgotPasswordViewModel) {
        this.forgotPasswordViewModel = forgotPasswordViewModel;
        this.forgotPasswordViewModel.addPropertyChangeListener(this);

        final JLabel title = new JLabel("Forgot Password");
        title.setAlignmentX(Component.CENTER_ALIGNMENT);

        final JPanel usernamePanel = new JPanel();
        final JLabel usernameLabel = new JLabel("Username:");
        usernamePanel.setLayout(new BoxLayout(usernamePanel, BoxLayout.Y_AXIS));
        usernameLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
        usernameInputField.setAlignmentX(Component.CENTER_ALIGNMENT);
        usernameInputField.setMaximumSize(usernameInputField.getPreferredSize());
        usernamePanel.add(usernameLabel);
        usernamePanel.add(usernameInputField);

        final JPanel securityquestionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        final JLabel securityquestionInfo = new JLabel("Security Question");
        securityquestion = new JLabel();
        securityquestionPanel.add(securityquestion);
        securityquestionPanel.add(securityquestionInfo);

        final JPanel securityanswerPanel = new JPanel();
        final JLabel securityanswerLabel = new JLabel("security question Answer");
        securityanswerPanel.setLayout(new BoxLayout(securityanswerPanel, BoxLayout.Y_AXIS));
        securityanswerLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
        securityanswerInputField.setAlignmentX(Component.CENTER_ALIGNMENT);
        securityanswerInputField.setMaximumSize(securityanswerInputField.getPreferredSize());
        securityanswerPanel.add(securityanswerLabel);
        securityanswerPanel.add(securityanswerInputField);

        final JPanel passwordPanel = new JPanel();
        final JLabel passwordLabel = new JLabel("New Password");
        passwordPanel.setLayout(new BoxLayout(passwordPanel, BoxLayout.Y_AXIS));
        passwordLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
        passwordInputField.setAlignmentX(Component.CENTER_ALIGNMENT);
        passwordInputField.setMaximumSize(passwordInputField.getPreferredSize());
        passwordPanel.add(passwordLabel);
        passwordPanel.add(passwordInputField);

        final JPanel button = new JPanel();
        setPassword = new JButton("Set Password");
        button.add(setPassword);

        this.setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        this.add(title);
        this.add(usernamePanel);
        this.add(securityquestionPanel);
        this.add(securityanswerPanel);
        this.add(passwordPanel);
        this.add(button);
    }

    // Below is Non-implemented

    public void propertyChange(PropertyChangeEvent evt) {
        if (evt.getPropertyName().equals("password")) {
            final ForgotPasswordState state = (ForgotPasswordState) evt.getNewValue();
            JOptionPane.showMessageDialog(null, "password updated for " + state.getUsername());
        }

    }

    public String getViewName() {
        return viewName;
    }

}