package pong;

import java.awt.Color;
import java.util.Scanner;
import javax.swing.JFrame;
import java.awt.GridLayout;

public class Main {
	public static void main(String []args) {
		JFrame frame  = new JFrame("Pong Game");
		
		PongGame pg = new PongGame();
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setBounds(50,25,700,540);
		frame.add(pg);
		frame.setVisible(true);
		frame.setResizable(false);
		
		
	}

}