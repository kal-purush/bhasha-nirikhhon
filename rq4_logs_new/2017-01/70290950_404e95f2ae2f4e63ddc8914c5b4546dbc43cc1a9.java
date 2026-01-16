
// Copyright Wintriss Technical Schools 2013
import javax.swing.JOptionPane;

public class BananaQuiz {

	public static void main(String[] args) {
		// 1. ask the user if they like bananas
		String boi = JOptionPane.showInputDialog("do you like Bananas?????");
		// 2. if they say no,
		// tell them they are crazy
		// and end quiz
		if (boi.equals("no")) {
			JOptionPane.showMessageDialog(null, "YOU ARE BANANAS!!");
		}

		// 3. if they say yes
		else if (boi.equals("yes")) {
			String yay = JOptionPane.showInputDialog("what is you're favorite hobby??");
		
		JOptionPane.showMessageDialog(null,   yay + " is much better with bananas!");
		}	// ask them what is their favorite hobby
		// show a pop up that says "<your hobby> is much better with bananas!"
		else{
			JOptionPane.showMessageDialog(null, "YOU ARE BANANAS");
		}
		// 4. OPTIONAL: if they say something other than “yes” or “no”
		// show a pop up that says “You are bananas!”
	}

}