import javax.swing.JFrame;
import javax.swing.SwingUtilities;
import javax.swing.JButton;

import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

class Menu extends JFrame {
	private static final long serialVersionUID = 1L;
	public static int level;

	public JButton Simple = new JButton("Simple");
	public JButton Hard = new JButton("Hard");

	public Menu() {
		getContentPane().setLayout(new FlowLayout());
		getContentPane().add(Simple);
		getContentPane().add(Hard);
		

		Simple.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				level = 1;
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						Main thisClass = new Main();
						thisClass.setLocationRelativeTo(null);
						thisClass.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
						thisClass.setVisible(true);
					}
				});

				// Close Menu once select Simple
				String cmd = e.getActionCommand();
				if (cmd.equals("Simple")) {
					dispose();
				}
			};
		});

		Hard.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				level = 2;
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						Main thisClass = new Main();
						thisClass.setLocationRelativeTo(null);
						thisClass.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
						thisClass.setVisible(true);
					}
				});

				// Close Menu once select Hard
				String cmd = e.getActionCommand();
				if (cmd.equals("Hard")) {
					dispose();
				}
			};
		});
	}
}