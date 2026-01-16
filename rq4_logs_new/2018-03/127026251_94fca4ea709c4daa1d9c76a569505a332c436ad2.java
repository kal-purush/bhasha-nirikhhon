package com.practice.gui;

import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

public class AddGUI {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Calc calculator = new Calc();
	}

}

class Calc extends JFrame
{
	JTextField t1,t2;
	JButton calculateB;
	JLabel resultsLabel;
	JTextField resultsField;
	JRadioButton addRB;
	JRadioButton subRB;
	public Calc(){
	
		
		t1 = new JTextField(20);
		t2 = new JTextField(20);
		addRB = new JRadioButton("Add");
		subRB = new JRadioButton("Sub");
		ButtonGroup bg = new ButtonGroup();
		bg.add(addRB);
		bg.add(subRB);
		
		calculateB = new JButton("Calculate");
		resultsLabel = new JLabel("Results");
		resultsField = new JTextField(5);
		resultsField.setEditable(false);
		add(t1);
		add(t2);
		add(addRB);
		add(subRB);
		add(calculateB);
		add(resultsLabel);
		add(resultsField);
		calculateB.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				// TODO Auto-generated method stub
				if(addRB.isSelected()){
					int num1 = Integer.parseInt(t1.getText());
					int num2 = Integer.parseInt(t2.getText());
					resultsField.setText(""+(num1+num2));
				}
				else {
					int num1 = Integer.parseInt(t1.getText());
					int num2 = Integer.parseInt(t2.getText());
					resultsField.setText(""+(num1-num2));
				}
				
			}
		});
		
		setLayout(new FlowLayout());
		setVisible(true);
		setSize(400,400);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}
}