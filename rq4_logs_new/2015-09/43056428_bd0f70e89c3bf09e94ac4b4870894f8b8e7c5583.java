import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class CounterControl extends JPanel {

	private CounterInterface cm;
	private CounterView panel;

	public CounterControl (CounterInterface cm) {
		this.cm = cm;
		panel = new CounterView(cm);
		JButton countButton = new JButton("Count");
		JButton resetButton = new JButton("Reset");
		countButton.addActionListener(new CountListener());
		resetButton.addActionListener(new ResetListener());
		setLayout(new BorderLayout());
		add(panel,BorderLayout.NORTH);
		add(countButton,BorderLayout.WEST);
		add(resetButton,BorderLayout.EAST);
		//setOpaque(true);
	}

	private class CountListener implements ActionListener {
		public void actionPerformed (ActionEvent e) {
			cm.increment();
			panel.repaint();
		}
	}    

	private class ResetListener implements ActionListener {
		public void actionPerformed (ActionEvent e) {
			cm.reset();
			panel.repaint();
		}
	}
}