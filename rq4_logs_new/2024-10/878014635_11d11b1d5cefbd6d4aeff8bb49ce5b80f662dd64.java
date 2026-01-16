import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

public class UserManagementApp extends JFrame {
    private final LoginService loginService;
    private final UserActionService userActionService;
    private JTextField usernameField;
    private JPasswordField passwordField;
    private JTextField emailField;
    private JComboBox<String> roleComboBox;
    private JButton logoutButton;
    private JButton deleteUserButton;
    private JCheckBox showPasswordCheckBox; // Thêm Checkbox để hiển thị mật khẩu
    private UserData loggedInUser;

    public UserManagementApp() {
        loginService = new LoginService();
        userActionService = new UserActionService();

        setTitle("User Management System");
        setSize(450, 450);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        // Title Panel
        JPanel titlePanel = new JPanel();
        JLabel titleLabel = new JLabel("User Management System");
        titleLabel.setFont(new Font("Arial", Font.BOLD, 24));
        titleLabel.setForeground(Color.WHITE);
        titlePanel.add(titleLabel);
        titlePanel.setBackground(new Color(50, 120, 220));
        titlePanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        add(titlePanel, BorderLayout.NORTH);

        // Main Panel
        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new GridLayout(7, 2, 10, 10)); // Thêm 1 dòng cho Checkbox
        add(mainPanel, BorderLayout.CENTER);

        // Change background color of main panel
        mainPanel.setBackground(new Color(230, 240, 255));

        // Username
        mainPanel.add(createStyledLabel("Username:"));
        usernameField = new JTextField();
        mainPanel.add(usernameField);

        // Password
        mainPanel.add(createStyledLabel("Password:"));
        passwordField = new JPasswordField();
        mainPanel.add(passwordField);

        // Show Password Checkbox
        showPasswordCheckBox = new JCheckBox("Show Password");
        showPasswordCheckBox.setBackground(new Color(230, 240, 255));
        showPasswordCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if (e.getStateChange() == ItemEvent.SELECTED) {
                    passwordField.setEchoChar((char) 0); // Hiển thị mật khẩu
                } else {
                    passwordField.setEchoChar('*'); // Ẩn mật khẩu (dấu chấm)
                }
            }
        });
        mainPanel.add(new JLabel()); // Add empty label to adjust grid layout
        mainPanel.add(showPasswordCheckBox);

        // Email
        mainPanel.add(createStyledLabel("Email:"));
        emailField = new JTextField();
        mainPanel.add(emailField);

        // Role Selection
        mainPanel.add(createStyledLabel("Role:"));
        roleComboBox = new JComboBox<>(new String[]{"Admin", "Moderator", "Regular User"});
        mainPanel.add(roleComboBox);

        // Button Panel
        JPanel buttonPanel = new JPanel();
        buttonPanel.setLayout(new FlowLayout());
        add(buttonPanel, BorderLayout.SOUTH);
        buttonPanel.setBackground(Color.LIGHT_GRAY);

        // Login Button with icon
        JButton loginButton = createStyledButton("Login", Color.GREEN.darker(), Color.BLACK);
        loginButton.addActionListener(new LoginButtonAction());
        buttonPanel.add(loginButton);

        // Register Button with icon
        JButton registerButton = createStyledButton("Register", new Color(0, 153, 204), Color.BLACK);
        registerButton.addActionListener(new RegisterButtonAction());
        buttonPanel.add(registerButton);

        // Logout Button (initially disabled)
        logoutButton = createStyledButton("Logout", Color.RED, Color.BLACK);
        logoutButton.addActionListener(new LogoutButtonAction());
        logoutButton.setEnabled(false);
        buttonPanel.add(logoutButton);

        // Delete User Button (initially disabled)
        deleteUserButton = createStyledButton("Delete User", Color.ORANGE, Color.BLACK);
        deleteUserButton.addActionListener(new DeleteUserButtonAction());
        deleteUserButton.setEnabled(false);
        buttonPanel.add(deleteUserButton);

        // Add padding
        mainPanel.setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));
        buttonPanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
    }

    // Method to create styled JLabel
    private JLabel createStyledLabel(String text) {
        JLabel label = new JLabel(text);
        label.setFont(new Font("Arial", Font.BOLD, 14));
        label.setForeground(new Color(50, 50, 150));
        return label;
    }

    // Method to create styled JButton
    private JButton createStyledButton(String text, Color backgroundColor, Color textColor) {
        JButton button = new JButton(text);
        button.setBackground(backgroundColor);
        button.setForeground(textColor);
        button.setFont(new Font("Arial", Font.BOLD, 14));
        button.setFocusPainted(false);
        button.setBorder(BorderFactory.createLineBorder(Color.WHITE, 2));
        return button;
    }

    private class LoginButtonAction implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            String username = usernameField.getText();
            String password = new String(passwordField.getPassword());

            try {
                loggedInUser = loginService.login(username, password);
                JOptionPane.showMessageDialog(null, "Login successful as " + loggedInUser.username());
                userActionService.handleUserAccess(loggedInUser);
                showUserInfo(); // Hiển thị thông tin người dùng
                toggleUserManagementButtons(true);
            } catch (IllegalArgumentException ex) {
                JOptionPane.showMessageDialog(null, ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private class RegisterButtonAction implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            String username = usernameField.getText();
            String email = emailField.getText();
            String password = new String(passwordField.getPassword());
            String selectedRole = (String) roleComboBox.getSelectedItem();

            User userType;
            switch (selectedRole) {
                case "Admin" -> userType = new Admin(username, email, password);
                case "Moderator" -> userType = new Moderator(username, email, password);
                case "Regular User" -> userType = new RegularUser(username, email, password);
                default -> throw new IllegalArgumentException("Invalid role selection");
            }

            try {
                loginService.register(username, email, password, userType);
                JOptionPane.showMessageDialog(null, "User " + username + " registered successfully.");
            } catch (IllegalArgumentException ex) {
                JOptionPane.showMessageDialog(null, ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private class LogoutButtonAction implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            loggedInUser = null;
            JOptionPane.showMessageDialog(null, "Logged out successfully.");
            toggleUserManagementButtons(false);
        }
    }

    private class DeleteUserButtonAction implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            if (loggedInUser.userType() instanceof Admin || loggedInUser.userType() instanceof Moderator) {
                String usernameToDelete = JOptionPane.showInputDialog("Enter username to delete:");
                if (usernameToDelete != null) {
                    try {
                        loginService.deleteUser(usernameToDelete);
                        JOptionPane.showMessageDialog(null, "User " + usernameToDelete + " deleted successfully.");
                    } catch (IllegalArgumentException ex) {
                        JOptionPane.showMessageDialog(null, ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                    }
                }
            } else {
                JOptionPane.showMessageDialog(null, "You do not have permission to delete users.", "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void toggleUserManagementButtons(boolean isLoggedIn) {
        logoutButton.setEnabled(isLoggedIn);
        deleteUserButton.setEnabled(isLoggedIn && (loggedInUser.userType() instanceof Admin || loggedInUser.userType() instanceof Moderator));
    }

    private void showUserInfo() {
        String userInfo = "User Info:\n" +
                "Username: " + loggedInUser.username() + "\n" +
                "Email: " + loggedInUser.email() + "\n" +
                "Role: " + loggedInUser.userType().getUserRole();
        JOptionPane.showMessageDialog(null, userInfo, "User Info", JOptionPane.INFORMATION_MESSAGE);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            UserManagementApp app = new UserManagementApp();
            app.setVisible(true);
        });
    }
}