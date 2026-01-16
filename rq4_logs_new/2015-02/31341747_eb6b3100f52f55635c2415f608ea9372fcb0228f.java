import javax.swing.JOptionPane;
public class Login {
	public static void main(String [] args) {
		String username1, password1, username2, password2;

		username1 = JOptionPane.showInputDialog("Please create a username.");
		password1 = JOptionPane.showInputDialog("Please create a password.");
		
		username2 = JOptionPane.showInputDialog("Please enter your username.");
		password2 = JOptionPane.showInputDialog("Please enter your password.");
	
		
		if (username1.equals(username2) && (password1.equals(password2))) {
		JOptionPane.showMessageDialog(null, "Welcome " + username1 + "!");

		} else 
			JOptionPane.showMessageDialog(null, "You have entered the username or password incorrectly. Please try again.");
		

		

			
		}
		
		
		
	}