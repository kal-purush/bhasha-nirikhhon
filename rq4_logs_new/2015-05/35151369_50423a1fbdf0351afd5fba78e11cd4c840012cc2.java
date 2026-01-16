package hust.samsung;

import java.awt.FlowLayout;
import java.awt.HeadlessException;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

public class Frame extends JFrame {
	private JFrame frame;
	private JButton btn1,btn2,exit;
	private JPanel panel;
	public Frame() throws HeadlessException{
		super();
		panel = new JPanel();
		panel.setLayout(new FlowLayout());
		btn1 = new JButton("Name");
		panel.add(btn1);
		btn2 = new JButton("ID");
		panel.add(btn2);
		exit = new JButton("Exit");
		panel.add(exit);
		setContentPane(panel);
		btn1.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent arg0) {
				JOptionPane.showMessageDialog(null, "I LOVE YOU");
			}
		});
		
		//Bat su kien cho button 2
		btn2.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				JOptionPane.showMessageDialog(null, "I MISS YOU");
				
			}
		});
		
		//Bat su kien cho button Exit
		exit.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				if ((JOptionPane.showConfirmDialog(null, "Do you want to exit?","warning",2) == 0)){
					System.exit(0);
				}
			}
		});
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setSize(300,150);
		setVisible(true);
	}
	
	public static void main(String[] args){
		try{
			Frame a= new Frame();
			
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
}