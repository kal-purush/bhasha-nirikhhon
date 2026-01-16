package test;

import javax.swing.*;

import java.util.Random;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class Pancheros {
	
	private static boolean wentYesterday = false;
	private static int odds = 80;
	
	public static void main(String[] args) {
		
		JFrame frame = new JFrame("Pancheros Decision");
		frame.setSize(500, 500);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		JLabel top = new JLabel("Go to pancheros today?");
		JPanel panel = new JPanel();
		JLabel decision = new JLabel("");
		
		panel.add(top);
		
		Random rando = new Random();
		
		JButton button = new JButton("decide.");
		panel.add(button);
		panel.add(decision);
		button.addActionListener(new ActionListener() {
									@Override
									public void actionPerformed(ActionEvent e) {
										if (wentYesterday) {
											odds--;
										} else {
											odds = 80;
											wentYesterday = false;
										}
										if (rando.nextInt(100) < odds) {
											System.out.println(odds);
											decision.setText("VAMANOS");
											wentYesterday = true;
										} else {
											System.out.println(odds);
											decision.setText("not today :(");
											wentYesterday = false;
										}
									}
								}
		);
		frame.setBackground(Color.white);
		frame.add(panel);
		frame.setVisible(true);
		
	}

}