import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
public class UserForm extends JFrame implements ActionListener {
    JTextField nameField, emailField, ageField;
    JButton submitBtn;
    public UserForm() {
        setTitle("User Information Form");
        setSize(350, 250);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setLayout(new GridLayout(5, 2, 10, 10));
        setLocationRelativeTo(null);
        add(new JLabel("Name:"));
        nameField = new JTextField();
        add(nameField);
        add(new JLabel("Email:"));
        emailField = new JTextField();
        add(emailField);
        add(new JLabel("Age:"));
        ageField = new JTextField();
        add(ageField);
        submitBtn = new JButton("Submit");
        submitBtn.addActionListener(this);
        add(submitBtn);
        add(new JLabel());
        setVisible(true);
    }
    public void actionPerformed(ActionEvent e) {
        // Read input values
        String name = nameField.getText();
        String email = emailField.getText();
        String age = ageField.getText();
        if (name.isEmpty() || email.isEmpty() || age.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Please fill all fields.", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }
        JOptionPane.showMessageDialog(this,
            "Name: " + name + "\nEmail: " + email + "\nAge: " + age,
            "Submitted Data",
            JOptionPane.INFORMATION_MESSAGE
        );
    }
    public static void main(String[] args) {
        new UserForm();
    }
}