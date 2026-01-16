package twitter.client;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;

public class TwitterW extends JFrame {

	public static void main(String s[]) {

		String categories[] = { "Household", "Office", "Extended Family", "Company (US)", "Company (World)", "Team",
				"Will", "Birthday Card List", "High School", "Country", "Continent", "Planet", "Birthday Card chool",
				"Country" }; // Just for test

		// Build Frame
		JFrame Twitterframe = new JFrame("TwitterW");
		Twitterframe.setSize(400, 500);
		Twitterframe.setLocationRelativeTo(null);
		Twitterframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		// SCrollPane + List
		JList list = new JList(categories);
		JScrollPane scrollPane = new JScrollPane(list);
		scrollPane.setPreferredSize(new Dimension(200, 300));
		scrollPane.setVisible(true);

		JPanel panel = new JPanel();
		// panel.setLayout(new FlowLayout());
		panel.setLayout(new GridLayout(1, 1, 1, 1));

		// Label
		JLabel label = new JLabel("Icone");
		label.setSize(48, 48);
		panel.add(label);
		panel.add(label, BorderLayout.WEST);

		// TextField
		JTextField textFieldStatus = new JTextField("...");
		panel.add(textFieldStatus);
		panel.add(textFieldStatus, BorderLayout.CENTER);

		// Button Update
		JButton buttonUpdate = new JButton();
		buttonUpdate.setText("Update");
		panel.add(buttonUpdate);
		panel.add(buttonUpdate, BorderLayout.EAST);

		Twitterframe.getContentPane().add(scrollPane, BorderLayout.CENTER);
		Twitterframe.setVisible(true);
		Twitterframe.add(panel);
		Twitterframe.getContentPane().add(panel, BorderLayout.SOUTH);

	}
}