package com.sysionng.numbertowordsgui;

import javax.swing.JLabel;
import javax.swing.JTextField;
import javax.swing.JButton;
import javax.swing.JTextArea;
import javax.swing.JFrame;
import javax.swing.JPanel;
import java.awt.Dimension;
import java.awt.Container;
import java.awt.Insets;
import java.awt.Font;
import java.awt.GridBagLayout;
import java.awt.GridBagConstraints;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import com.sysionng.numbertowordsgui.Number2WordsApp;

public class Number2WordsAppGui implements ActionListener {
	private JLabel jlNumberToConvert;
	private JTextField jtNumberToConvert;
	private JLabel jlNumberInWords;
	private JTextArea jtaNumberInWords;
	private JButton jbConvert;
	private JButton jbClear;
	private JButton jbSearch;
	private JButton jbDelete;

	public Number2WordsAppGui(){
		JFrame numFrame = initContainer();
		numFrame.setVisible(true);
	}

	private JFrame initContainer(){
		JFrame frame = new JFrame("Number2Words GUI");
		frame.setSize(300,200); 
		frame.setLocation(10,200); 
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setResizable(false);
		frame.setLocationRelativeTo(null);

		Container contentPane = frame.getContentPane();
		contentPane.add(initComponents());

		frame.pack();

		return frame;
	}

	private JPanel initComponents(){

		JPanel panel = new JPanel();
		panel.setPreferredSize(new Dimension(350, 250));
		panel.setLayout(new GridBagLayout());
		GridBagConstraints gbc = new GridBagConstraints();

		jlNumberToConvert = new JLabel("Enter Number");
		jtNumberToConvert = new JTextField();
		jtNumberToConvert.setPreferredSize(new Dimension(200,20));
		jtNumberToConvert.setFont(jtNumberToConvert.getFont().deriveFont(Font.BOLD, 14));
		//jtNumberToConvert.setFont(new Font("Tahoma", Font.BOLD, 14));

		jlNumberInWords = new JLabel("Number in Words:");
		jtaNumberInWords = new JTextArea();
		jtaNumberInWords.setLineWrap(true);
		jtaNumberInWords.setPreferredSize(new Dimension(300,70));
		jtaNumberInWords.setEditable(false);
		jtaNumberInWords.setFont(jtNumberToConvert.getFont().deriveFont(Font.BOLD, 14));

		jbConvert = new JButton("Convert");
		jbConvert.setPreferredSize(new Dimension(150,20));
		jbClear = new JButton("Clear");
		jbClear.setPreferredSize(new Dimension(150,20));

		jbConvert.addActionListener(this);
		jbClear.addActionListener(this);

		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.gridwidth = 1; 		//span one(1) cell/column in x-direction
		gbc.gridheight = 1;	
		gbc.ipadx = 5;			//equivalent to paddingLeft+paddingRight
		gbc.ipady = 5;			//equivalent to paddingTop+paddingBottom
		gbc.insets = new Insets(10, 5, 5, 5);		//equivalent to marginTop, marginLeft, marginBottom and marginRight
		panel.add(jlNumberToConvert, gbc);

		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.gridx = 0;
		gbc.gridy = 1;
		gbc.gridwidth = 2; 		//span two(2) cells/columns in x-direction
		gbc.gridheight = 1;	
		gbc.ipadx = 10;			//equivalent to paddingLeft+paddingRight
		gbc.ipady = 10;			//equivalent to paddingTop+paddingBottom
		gbc.insets = new Insets(5, 5, 5, 5);		//equivalent to marginTop, marginLeft, marginBottom and marginRight
		panel.add(jtNumberToConvert, gbc);

		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.gridx = 0;
		gbc.gridy = 2;
		gbc.gridwidth = 1; 		//span one(1) cell/column in x-direction
		gbc.gridheight = 1;	
		gbc.ipadx = 5;			//equivalent to paddingLeft+paddingRight
		gbc.ipady = 5;			//equivalent to paddingTop+paddingBottom
		gbc.insets = new Insets(10, 5, 5, 5);		//equivalent to marginTop, marginLeft, marginBottom and marginRight
		panel.add(jlNumberInWords, gbc);

		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.gridx = 0;
		gbc.gridy = 3;
		gbc.gridwidth = 2; 		//span two(2) cells/columns in x-direction
		gbc.gridheight = 1; 	//span one(1) cell/column in y-direction
		gbc.ipadx = 10;			//equivalent to paddingLeft+paddingRight
		gbc.ipady = 10;			//equivalent to paddingTop+paddingBottom
		gbc.insets = new Insets(5, 5, 5, 5);		//equivalent to marginTop, marginLeft, marginBottom and marginRight
		panel.add(jtaNumberInWords, gbc);

		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.gridx = 0;
		gbc.gridy = 4;
		gbc.gridwidth = 1; 		//span one(1) cell/column in x-direction
		gbc.gridheight = 1;	
		gbc.weightx = 1; 	
		gbc.insets = new Insets(10, 5, 5, 5);		//equivalent to marginTop, marginLeft, marginBottom and marginRight	
		panel.add(jbConvert, gbc);

		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.gridx = 1;
		gbc.gridy = 4;
		gbc.gridwidth = 1; 		//span one(1) cell/column in x-direction
		gbc.gridheight = 1;	
		gbc.weightx = 1; 	
		gbc.insets = new Insets(10, 5, 5, 5);		//equivalent to marginTop, marginLeft, marginBottom and marginRight	
		panel.add(jbClear, gbc);
		
		return panel;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		switch (e.getActionCommand()){
			case "Convert":
				String numberInput = jtNumberToConvert.getText().toString().trim();

				if (Number2WordsUtility.GetNumber(numberInput) == -1) {
					jtaNumberInWords.setText("Invalid input entered by user");
				}
				else {
					Number2Words mNumber2Words = new Number2Words(Number2WordsUtility.GetNumber(numberInput));
					String mNumberInWords = mNumber2Words.ConvertNumber2Words();
					String firstLetterCapital = mNumberInWords.substring(0, 1).toUpperCase() + mNumberInWords.substring(1);
					jtaNumberInWords.setText(firstLetterCapital);
				}
				break;
			case "Clear":
				jtNumberToConvert.setText("");
				jtaNumberInWords.setText("");
				break;
		}
		
    }



}//