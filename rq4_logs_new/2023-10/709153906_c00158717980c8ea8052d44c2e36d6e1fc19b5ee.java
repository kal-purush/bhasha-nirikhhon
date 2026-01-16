package Wordcount;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import java.awt.Color;
import javax.swing.JLabel;
import javax.swing.JTextField;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.awt.Font;

public class WordCounter extends JFrame {

	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JTextField txt;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					WordCounter frame = new WordCounter();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public WordCounter() {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		contentPane = new JPanel();
		contentPane.setBackground(Color.PINK);
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));

		setContentPane(contentPane);
		contentPane.setLayout(null);
		
		JLabel la = new JLabel("Number of Words in the TextField : ");
		la.setFont(new Font("Rockwell Extra Bold", Font.BOLD, 14));
		la.setBounds(43, 44, 362, 23);
		contentPane.add(la);
		
		txt = new JTextField();
		txt.setBounds(64, 93, 240, 115);
		contentPane.add(txt);
		txt.setColumns(10);
		
		JButton bt = new JButton("Check");
		bt.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(e.getSource()==bt)
				{
					String text=txt.getText();
					//la.setText("Word"+text.length());
					String words[]=text.split("\\s");
					la.setText("Number of Words in the TextField: "+words.length);
					
					
				}
			}
		});
		bt.setBounds(133, 227, 89, 23);
		contentPane.add(bt);
	}

}