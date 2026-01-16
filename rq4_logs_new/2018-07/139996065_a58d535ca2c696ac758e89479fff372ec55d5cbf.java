/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ccmspb;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

/**
 *
 * @author HP
 */
public class AllDisposedCases {
	JPanel centerPanel;	//contains main data input and required output
	BorderLayout centerPanelLayout;
	
	JPanel inputPanel;	//inputs required details
	GridBagLayout inputPanelLayout;
	
	ButtonGroup radioButtons;	//for selecting only one radio button at at time
	JRadioButton forCourtWise, forPeriodWise;
	
	JComboBox selectCourt;	//drop-down box
	
	JLabel fromDate, toDate;
	JTextField fromDateField, toDateField;
	
	JButton submit;
	
	JPanel outputPanel;//produces output based on the provided input
		
	void prepareGUI(){	
	/**center panel layout begins*/
		centerPanel = new JPanel();
		centerPanelLayout = new BorderLayout();
		centerPanel.setLayout(centerPanelLayout);
		
		/**input panel layout begins*/
		inputPanel = new JPanel();
		inputPanelLayout = new GridBagLayout();
		inputPanel.setLayout(inputPanelLayout);
		
		GridBagConstraints inputConstraint = new GridBagConstraints();
                inputConstraint.gridheight = 1;
                inputConstraint.gridwidth = 1;
                inputConstraint.gridx = 0;
                inputConstraint.gridy = 0;
                inputConstraint.insets.top = 5;
                inputConstraint.insets.bottom = 5;
                inputConstraint.insets.left = 5;
                inputConstraint.insets.right = 8;
                inputConstraint.anchor = GridBagConstraints.WEST;
		
		radioButtons = new ButtonGroup();	//for selecting one button at a time
		
		forCourtWise = new JRadioButton("Court Wise", false);	//button would be off by default
		radioButtons.add(forCourtWise);
                inputPanel.add(forCourtWise, inputConstraint);
		
                inputConstraint.gridx++;		
		
		String[] courts = {"Supreme Court", "High Court", "District Court"};	//all courts in the drop-down
		
		selectCourt = new JComboBox(courts);
		inputPanel.add(selectCourt);
				
		inputConstraint.gridx = 0;
		inputConstraint.gridy++;
		
		forPeriodWise = new JRadioButton("Listed Over Period", false);	//button would be off by default
		radioButtons.add(forPeriodWise);
		inputPanel.add(forPeriodWise, inputConstraint);
		
		inputConstraint.gridx++;
		fromDate = new JLabel("From Date");
		inputPanel.add(fromDate, inputConstraint);
		
		inputConstraint.gridx++;
		fromDateField = new JTextField();
		fromDateField.setColumns(7);
		inputPanel.add(fromDateField, inputConstraint);
		
		inputConstraint.gridx = 0;
		inputConstraint.gridy++;
		inputConstraint.gridx++;
		toDate = new JLabel("To Date");
		inputPanel.add(toDate, inputConstraint);
		
		inputConstraint.gridx++;
		toDateField = new JTextField();
		toDateField.setColumns(7);
		inputPanel.add(toDateField, inputConstraint);
		
		inputConstraint.gridx = 0;
		inputConstraint.gridy++;
		inputConstraint.gridx++;
		submit = new JButton("Submit");
		inputPanel.add(submit, inputConstraint);
		
		centerPanel.add(inputPanel, BorderLayout.NORTH);
		/**input panel layout completed*/
		
		/**output panel layout begins*/
		outputPanel = new JPanel();
		outputPanel.setBackground(Color.lightGray);
		centerPanel.add(outputPanel, BorderLayout.CENTER);
		/**output panel layout completed*/
	/**center panel layout completed*/
	}
}