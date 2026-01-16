package treadmillWithDb;

import java.awt.Container;
import java.awt.EventQueue;

import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JLabel;
import javax.swing.JButton;
import javax.swing.SwingConstants;
import javax.swing.JTextField;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.Timer;

import java.awt.Color;
import java.awt.Font;
import java.awt.SystemColor;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.nio.file.Watchable;
import java.sql.Time;
import java.text.DecimalFormat;


/**
 * @author Szabolcs Kovacs	Student# B00063874
 * Group: Grp 1				Date: March 2014
 * Treadmill Project		Year 2
 * COMP H2027 - Software Engineering and Testing
 *
 * Copyright (c)
 *
 * Controller.java class is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 */
 

public class ControllerWindow extends JInternalFrame implements ActionListener, Runnable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Attributes -- Elements of the GUI
	 */
	private JPanel pnlMain, pnlLeft, pnlLeftTitle, pnlRight, panelOptions, pnlMiddleTop, pnlMiddBottom;
	private JLabel lblBrand, lblLeftTitle, lblOptions, lblHeartRate, lblTime, lblCalories, lblDistance,lblSpeed;
	private JButton btnManual, btnPerformance, btnInterval, btnFitness, btnStart, btnReset, btnPause;
	private JButton btnStop, btnTime, btnGradient, btnSpeed, btnCalories, btnDistance, btnAdd, btnSubtract;
	private JButton btnSave;
	private JTextField txtFldHeartRate, txtFldTime, txtFldCalories, txtFldGradient, txtFldSpeed, txtFldDistance;
	JTextArea txtAreaMain;
	
	Double timerDouble = 0.00;
	String timerString = "";
	Double gradientDouble = 0.00;
	String gradientString = "";
	Float speedFloat = 0.0f;
	String speedString = "";
	int caloriesInt = 0;
	String caloriesString = "";
	int distanceInt =0;
	String distanceString = "";
	
	int secondInt = 59;
	int minuteInt;
	String secondString = "";
	String minuteString = "";
	int tempMinute;
	int timeLength;
	Timer timer;
	private Thread counterThread = null;
	
	boolean btnStartPressed = false;
	boolean btnTimePressed = false;
	boolean btnGradientPressed = false;
	boolean btnSpeedPressed = false;
	boolean btnCaloriesPressed = false;
	boolean btnDistancePressed = false;
	boolean btnManualPressed = false;
	boolean btnSavePressed = false;
	boolean btnStopPressed = false;
	boolean btnPerformancePressed = false;
	boolean btnIntervalPressed = false;
	boolean btnFitnessPressed = false;
	boolean btnResetPressed = false;
	boolean btnPausePressed = false;
	
	/**
	 * Create the frame.
	 */
	public ControllerWindow(int[] sessData, String patientName) {
		super("Controller Window", false, false, false, false);
		setBounds(45, 00, 800, 550);
//--------------------- Inner Panel ---------------------------------------------------------------------------------
		pnlMain = new JPanel();
		pnlMain.setBackground(SystemColor.control);
		pnlMain.setBounds(0, 11, 784, 510);
		getContentPane().add(pnlMain);
		pnlMain.setLayout(null);
		
//--------------- Header ------------------------------------------------------------------------		
		JPanel pnlMain_1 = new JPanel();
		pnlMain_1.setBackground(Color.DARK_GRAY);
		pnlMain_1.setBounds(10, 11, 764, 45);
		pnlMain.add(pnlMain_1);
		
		lblBrand = new JLabel("Precor");
		pnlMain_1.add(lblBrand);
		lblBrand.setForeground(Color.WHITE);
		lblBrand.setFont(new Font("Tekton Pro", Font.PLAIN, 34));
		
//----------------------- Left Panel and Buttons -----------------------------------------------------------		
		pnlLeft = new JPanel();
		pnlLeft.setBackground(SystemColor.control);
		pnlLeft.setBounds(10, 67, 201, 439);
		pnlMain.add(pnlLeft);
		pnlLeft.setLayout(null);
		
		pnlLeftTitle = new JPanel();
		pnlLeftTitle.setBackground(Color.DARK_GRAY);
		pnlLeftTitle.setForeground(Color.DARK_GRAY);
		pnlLeftTitle.setBounds(10, 11, 181, 50);
		pnlLeft.add(pnlLeftTitle);
		
		lblLeftTitle = new JLabel("Programs");
		pnlLeftTitle.add(lblLeftTitle);
		lblLeftTitle.setForeground(Color.WHITE);
		lblLeftTitle.setBackground(Color.DARK_GRAY);
		lblLeftTitle.setHorizontalAlignment(SwingConstants.CENTER);
		lblLeftTitle.setFont(new Font("Meiryo", Font.PLAIN, 16));
		
		btnManual = new JButton("Manual");
		btnManual.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(e.getSource() == btnManual) {		
// set all the fields to the starting point --it is waiting until start button is pressed
	//				if((timerDouble == 0) && (gradientDouble == 0 ) && (caloriesInt == 0) && (distanceInt == 0)) {
						startingSettings();
						manualSetStartingPosition();
		//			}
				}
			}
		});
		btnManual.setForeground(Color.WHITE);
		btnManual.setFont(new Font("Meiryo", Font.PLAIN, 14));
		btnManual.setBackground(Color.DARK_GRAY);
		btnManual.setBounds(10, 90, 181, 40);
		pnlLeft.add(btnManual);
		
		btnPerformance = new JButton("Performance");
		/**
		 * Action listener handles the performance button. 
		 * When the button is press the controller unit sets 
		 * all its fields (factory settings)
		 *  
		 */
		btnPerformance.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				startingSettings();
				performanceSetStartingPosition();
	  //		btnStart.setEnabled(true);
				
			}
		});
		btnPerformance.setForeground(Color.WHITE);
		btnPerformance.setFont(new Font("Meiryo", Font.PLAIN, 14));
		btnPerformance.setBackground(Color.DARK_GRAY);
		btnPerformance.setBounds(10, 161, 181, 40);
		pnlLeft.add(btnPerformance);
		
		btnInterval = new JButton("Interval");
		btnInterval.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				startingSettings();
				intervalSetStartingPosition();
			} 
		});
		btnInterval.setForeground(Color.WHITE);
		btnInterval.setBackground(Color.DARK_GRAY);
		btnInterval.setFont(new Font("Meiryo", Font.PLAIN, 14));
		btnInterval.setBounds(10, 228, 181, 40);
		pnlLeft.add(btnInterval);
		
		btnFitness = new JButton("Fitness");
		btnFitness.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				startingSettings();
				fitnessSetStartingPosition();
			}
		});
		btnFitness.setForeground(Color.WHITE);
		btnFitness.setFont(new Font("Meiryo", Font.PLAIN, 14));
		btnFitness.setBackground(Color.DARK_GRAY);
		btnFitness.setBounds(10, 296, 181, 40);
		pnlLeft.add(btnFitness);
		
		btnReset = new JButton("RESET");
		btnReset.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(!btnStartPressed) {
					if(JOptionPane.showConfirmDialog(null, "Are you sure you want to reset before save this session", 
						null, JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.WARNING_MESSAGE) == JOptionPane.YES_OPTION) {
						resetAllButtons(); // reset all the fileds and buttons
						minuteInt = 0;
			//			tempMinute = 0;
				}
		}
			}
		});
		btnReset.setBackground(Color.DARK_GRAY);
		btnReset.setForeground(new Color(255, 255, 51));
		btnReset.setFont(new Font("Meiryo", Font.PLAIN, 18));
		btnReset.setEnabled(false);
		btnReset.setBounds(10, 368, 89, 60);
		pnlLeft.add(btnReset);
		
		btnPause = new JButton("PAUSE");
		btnPause.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				timer.stop();
			}
		});
		btnPause.setBackground(Color.DARK_GRAY);
		btnPause.setForeground(new Color(255, 153, 51));
		btnPause.setFont(new Font("Tahoma", Font.PLAIN, 18));
		btnPause.setEnabled(false);
		btnPause.setBounds(102, 368, 89, 60);
		pnlLeft.add(btnPause);
		
//------------------ Right Panel and Buttons --------------------------------------------------------------
		pnlRight = new JPanel();
		pnlRight.setBackground(SystemColor.control);
		pnlRight.setBounds(573, 67, 201, 439);
		pnlMain.add(pnlRight);
		pnlRight.setLayout(null);
		
		panelOptions = new JPanel();
		panelOptions.setBackground(Color.DARK_GRAY);
		panelOptions.setBounds(10, 11, 181, 50);
		pnlRight.add(panelOptions);
		
		lblOptions = new JLabel("Options");
		lblOptions.setForeground(Color.WHITE);
		lblOptions.setHorizontalAlignment(SwingConstants.CENTER);
		lblOptions.setFont(new Font("Meiryo", Font.PLAIN, 16));
		panelOptions.add(lblOptions);
// Set up the length of the exercise time
		btnTime = new JButton("Time");
		btnTime.addActionListener(this);
		btnTime.setForeground(Color.WHITE);
		btnTime.setBackground(Color.DARK_GRAY);
		btnTime.setFont(new Font("Meiryo", Font.PLAIN, 12));
		btnTime.setEnabled(false);
		btnTime.setBounds(10, 85, 85, 35);
		pnlRight.add(btnTime);
		
		btnGradient = new JButton("Gradient");
		btnGradient.setBackground(Color.DARK_GRAY);
		btnGradient.setForeground(Color.WHITE);
		btnGradient.setFont(new Font("Meiryo", Font.PLAIN, 12));
		btnGradient.setBounds(108, 85, 85, 35);
		btnGradient.setEnabled(false);
		btnGradient.addActionListener(this);
		pnlRight.add(btnGradient);
		
		btnSpeed = new JButton("Speed");
		btnSpeed.setBackground(Color.DARK_GRAY);
		btnSpeed.setForeground(Color.WHITE);
		btnSpeed.setFont(new Font("Meiryo", Font.PLAIN, 12));
		btnSpeed.setBounds(10, 131, 89, 35);
		btnSpeed.setEnabled(false);
		btnSpeed.addActionListener(this);
		pnlRight.add(btnSpeed);
		
		btnCalories = new JButton("Calories");
		btnCalories.setBackground(Color.DARK_GRAY);
		btnCalories.setForeground(Color.WHITE);
		btnCalories.setFont(new Font("Meiryo", Font.PLAIN, 12));
		btnCalories.setBounds(108, 131, 83, 35);
		btnCalories.setEnabled(false);
		btnCalories.addActionListener(this);
		pnlRight.add(btnCalories);
		
		btnDistance = new JButton("Distance");
		btnDistance.setBackground(Color.DARK_GRAY);
		btnDistance.setForeground(Color.WHITE);
		btnDistance.setFont(new Font("Meiryo", Font.PLAIN, 12));
		btnDistance.setBounds(62, 177, 89, 35);
		btnDistance.setEnabled(false);
		btnDistance.addActionListener(this);
		pnlRight.add(btnDistance);
		
		btnAdd = new JButton("+");
		btnAdd.setForeground(Color.WHITE);
		btnAdd.setBackground(Color.BLUE);
		btnAdd.setFont(new Font("Meiryo", Font.PLAIN, 20));
		btnAdd.setBounds(82, 223, 50, 50);
		btnAdd.setEnabled(false);
		btnAdd.addActionListener(this);
		pnlRight.add(btnAdd);
		
		btnSubtract = new JButton("-");
		btnSubtract.setForeground(Color.WHITE);
		btnSubtract.setBackground(Color.BLUE);
		btnSubtract.setFont(new Font("Meiryo", Font.PLAIN, 20));
		btnSubtract.setBounds(82, 284, 50, 50);
		btnSubtract.setEnabled(false);
		btnSubtract.addActionListener(this);
		pnlRight.add(btnSubtract);
		
		btnSave = new JButton("SAVE");
		btnSave.setBackground(Color.DARK_GRAY);
		btnSave.setForeground(new Color(153, 255, 51));
		btnSave.setFont(new Font("Meiryo", Font.PLAIN, 18));
		btnSave.setEnabled(false);
		btnSave.setBounds(10, 369, 181, 59);
		pnlRight.add(btnSave);
//------------ Middle top Panel --------------------------------------------------------------------------
		
		pnlMiddleTop = new JPanel();
		pnlMiddleTop.setBackground(Color.DARK_GRAY);
		pnlMiddleTop.setBounds(221, 67, 344, 63);
		pnlMain.add(pnlMiddleTop);
		pnlMiddleTop.setLayout(null);
		
		lblHeartRate = new JLabel("Heart rate");
		lblHeartRate.setForeground(Color.WHITE);
		lblHeartRate.setHorizontalAlignment(SwingConstants.CENTER);
		lblHeartRate.setFont(new Font("Meiryo", Font.PLAIN, 12));
		lblHeartRate.setBounds(5, 11, 86, 14);
		pnlMiddleTop.add(lblHeartRate);
		
		txtFldHeartRate = new JTextField();
		txtFldHeartRate.setText("-----------------");
		txtFldHeartRate.setEditable(false);
		txtFldHeartRate.setFont(new Font("Monospaced", Font.PLAIN, 14));
		txtFldHeartRate.setBounds(5, 24, 86, 28);
		txtFldHeartRate.setHorizontalAlignment(JTextField.CENTER);
		pnlMiddleTop.add(txtFldHeartRate);
		txtFldHeartRate.setColumns(10);
		
		lblTime = new JLabel("Time");
		lblTime.setHorizontalAlignment(SwingConstants.CENTER);
		lblTime.setForeground(Color.WHITE);
		lblTime.setFont(new Font("Meiryo", Font.PLAIN, 12));
		lblTime.setBounds(112, 11, 120, 14);
		pnlMiddleTop.add(lblTime);
		
		txtFldTime = new JTextField();
		txtFldTime.setEditable(false);
		txtFldTime.setText("-- / --");
		txtFldTime.setFont(new Font("Monospaced", Font.PLAIN, 14));
		txtFldTime.setBounds(112, 24, 120, 28);
		txtFldTime.setHorizontalAlignment(JTextField.CENTER);
		pnlMiddleTop.add(txtFldTime);
		txtFldTime.setColumns(10);
		
		txtFldCalories = new JTextField();
		txtFldCalories.setEditable(false);
		txtFldCalories.setText("-----------------");
		txtFldCalories.setFont(new Font("Monospaced", Font.PLAIN, 14));
		txtFldCalories.setBounds(253, 24, 86, 28);
		txtFldCalories.setHorizontalAlignment(JTextField.CENTER);
		pnlMiddleTop.add(txtFldCalories);
		txtFldCalories.setColumns(10);
		
		lblCalories = new JLabel("Calories");
		lblCalories.setHorizontalAlignment(SwingConstants.CENTER);
		lblCalories.setForeground(Color.WHITE);
		lblCalories.setFont(new Font("Meiryo", Font.PLAIN, 12));
		lblCalories.setBounds(248, 11, 90, 14);
		pnlMiddleTop.add(lblCalories);
		
		JLabel lblNewLabel = new JLabel("New label");
		lblNewLabel.setBounds(151, 32, 46, 14);
		pnlMiddleTop.add(lblNewLabel);
//-------------------- Middle of the interface --Display data from database ------------------------------------------ 
		txtAreaMain = new JTextArea();
		txtAreaMain.setFont(new Font("Monospaced", Font.PLAIN, 12));
		txtAreaMain.setForeground(Color.BLACK);
		txtAreaMain.setEditable(false);
		txtAreaMain.setBounds(221, 140, 344, 198);
		pnlMain.add(txtAreaMain);
		
		/////////////////////////////////////////////////////////////////////////////////////////////added this
		txtAreaMain.setText("\tSession Parameters\n");
		txtAreaMain.append("\tPatient Name:" + patientName + "\n\n");
		txtAreaMain.append("\n\tSpeed :" + sessData[0]);
		txtAreaMain.append("\n\tGradient :" + sessData[1]);
		txtAreaMain.append("\n\tTime :" + sessData[2]);
		
		
//------------------------Middle Panel displays gradient, speed, and distance--------------------------------------		
		pnlMiddBottom = new JPanel();
		pnlMiddBottom.setBackground(Color.DARK_GRAY);
		pnlMiddBottom.setBounds(221, 349, 344, 74);
		pnlMain.add(pnlMiddBottom);
		pnlMiddBottom.setLayout(null);
	
		JLabel lblGradient = new JLabel("Gradient (%)");
		lblGradient.setBounds(0, 5, 91, 22);
		pnlMiddBottom.add(lblGradient);
		lblGradient.setHorizontalAlignment(SwingConstants.CENTER);
		lblGradient.setForeground(Color.WHITE);
		lblGradient.setFont(new Font("Meiryo", Font.PLAIN, 12));
		
		txtFldGradient = new JTextField();
		txtFldGradient.setEditable(false);
		txtFldGradient.setText("-----------------");
		txtFldGradient.setFont(new Font("Monospaced", Font.PLAIN, 14));
		txtFldGradient.setBounds(5, 26, 86, 37);
		txtFldGradient.setHorizontalAlignment(JTextField.CENTER);
		pnlMiddBottom.add(txtFldGradient);
		txtFldGradient.setColumns(10);
		
		lblSpeed = new JLabel("Speed (km/h)");
		lblSpeed.setHorizontalAlignment(SwingConstants.CENTER);
		lblSpeed.setForeground(Color.WHITE);
		lblSpeed.setFont(new Font("Meiryo", Font.PLAIN, 12));
		lblSpeed.setBounds(110, 9, 121, 14);
		pnlMiddBottom.add(lblSpeed);
		
		txtFldSpeed = new JTextField();
		txtFldSpeed.setEditable(false);
		txtFldSpeed.setText("-----------------------");
		txtFldSpeed.setFont(new Font("Monospaced", Font.PLAIN, 14));
		txtFldSpeed.setBounds(111, 26, 120, 37);
		txtFldSpeed.setHorizontalAlignment(JTextField.CENTER);
		pnlMiddBottom.add(txtFldSpeed);
		txtFldSpeed.setColumns(10);
		
		lblDistance = new JLabel("Distance (m)");
		lblDistance.setHorizontalAlignment(SwingConstants.CENTER);
		lblDistance.setForeground(Color.WHITE);
		lblDistance.setFont(new Font("Meiryo", Font.PLAIN, 12));
		lblDistance.setBounds(253, 9, 86, 14);
		pnlMiddBottom.add(lblDistance);
		
		txtFldDistance = new JTextField();
		txtFldDistance.setEditable(false);
		txtFldDistance.setText("-----------------------");
		txtFldDistance.setFont(new Font("Digital", Font.PLAIN, 14));
		txtFldDistance.setBounds(253, 26, 86, 37);
		txtFldDistance.setHorizontalAlignment(JTextField.CENTER);
		pnlMiddBottom.add(txtFldDistance);
		txtFldDistance.setColumns(10);
//------------------------------- Start and Stop Buttons ---------------------------------------------------------------------------		
		btnStop = new JButton("STOP");
		btnStop.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				timer.stop();
				btnStartPressed = false;
				btnSave.setEnabled(true);
				timeLength = tempMinute - minuteInt;
				System.out.println("TimeLength: " +timeLength);
				System.out.println("minuteInt: " + minuteInt);
				System.out.println("tempMinute: " + tempMinute);
			}
		});
		btnStop.setBounds(405, 434, 163, 66);
		pnlMain.add(btnStop);
		btnStop.setBackground(new Color(255, 0, 0));
		btnStop.setForeground(Color.WHITE);
		btnStop.setEnabled(false);
		btnStop.setFont(new Font("Meiryo", Font.PLAIN, 24));
		
		btnStart = new JButton("START");
		btnStart.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				startButtonStarting();
				timer.start();				
			}
		});
		btnStart.setBounds(227, 434, 163, 66);
		pnlMain.add(btnStart);
		btnStart.setBackground(new Color(0, 128, 0));
		btnStart.setForeground(Color.WHITE);
		btnStart.setEnabled(false);
		btnStart.setFont(new Font("Meiryo", Font.PLAIN, 24));
		btnStart.addActionListener(this);
		getContentPane().setLayout(null);

		setVisible(true);
		Container c = getContentPane();
		c.add(pnlMain);
	    
/**
 * 	The timer class is responsible for counting down the time.  
 */
//		minuteInt = (int)Math.round(timerDouble);
//		tempMinute = (int)Math.round(timerDouble); 
		ActionListener timerListener = new ActionListener() {

			public void actionPerformed(ActionEvent evt) {
	    		if(minuteInt == tempMinute) {
	    			minuteInt = (int)Math.round(timerDouble)-1;
	    		}
				
	    		if((secondInt > 0 ) && (secondInt < 61)) {
	    					secondInt -= 1;
	    		}
	    		else {
	    			secondInt = 59;
	    			minuteInt -=1;
	    		}
	    		tempMinute = (int)Math.round(timerDouble); 
	    		minuteString = Integer.toString(minuteInt);
	    		secondString = Integer.toString(secondInt);
	    		if(secondInt < 10) {
	    			txtFldTime.setText(minuteString + ":0" + secondString);
	    		}
	    		else {
	    				txtFldTime.setText(minuteString + ":" + secondString);
	    		}
	    	}
	    	    };    	     
	    	    timer = new Timer(1000,timerListener);	    	      
	}

/**
 * ActionListener  
 */
	@Override
	public void actionPerformed(ActionEvent e) {
		if(speedFloat < 1) {
			btnStart.setEnabled(false);
		}
		if(e.getSource() == btnStart) {
			btnStartPressed = true;   // change the state of the start button
		}
// If the manual button selected -- user able to customize the controller
		if(e.getSource() == btnTime) {
			optionButtonState();   // change all buttons's state to false
			btnTimePressed = true;
			buttonEnabledSet(); //  add, subtract, pause buttons are enabled 
		}
// if the time button was pressed, the user able to use the "+" or "-" buttons to change the timer by +/- 1
		if(btnTimePressed) {
			if((e.getSource() == btnAdd)  && (timerDouble < 60.00)) {
				timerDouble = timerDouble + 1.00;
				timerString = Double.toString(timerDouble);
				System.out.println("Test when the time increases or not by 1 = " + timerDouble);
				txtFldTime.setText(timerString);
			}
// "-" decreases the timer by -1 
			else if((e.getSource() == btnSubtract) && (timerDouble > 0.00))  {
				timerDouble = timerDouble - 1.00;
				timerString = Double.toString(timerDouble);
				txtFldTime.setText(timerString);
				System.out.println("Test when the time increases or not by 1 = " + timerDouble);
			}
			else if((e.getSource() == btnAdd) && (timerDouble == 60.00)) {
				String warnMessage = "The maximum time is 60 minutes";
				String warnTitle = "Invalid user's input";
				JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
			}
			else if((e.getSource() == btnSubtract) && (timerDouble == 0.00)) {
				String warnMessage = "The minimum time is 0 minutes";
				String warnTitle = "Invalid user's input";
				JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
			}
		}
// Gradient button is pressed
		if(e.getSource() == btnGradient) {
			optionButtonState(); // change all buttons' state to false
//			btnTimePressed = false;
			btnGradientPressed = true;
			System.out.println("button gradien " + btnGradientPressed);
			buttonEnabledSet(); //  add, subtract, pause buttons are enabled 
		}
// if the time button was pressed, the user able to use the "+" or "-" buttons to change the timer by +/- 1
		if(btnGradientPressed) {
			if((e.getSource() == btnAdd)  && (gradientDouble < 15.00)) {
				gradientDouble = gradientDouble + 1;
				gradientString = Double.toString(gradientDouble);
				System.out.println("Test when the time increases or not by 1 = " + gradientDouble);
				txtFldGradient.setText(gradientString);
			}
// "-" decreases the timer by -1 
			else if((e.getSource() == btnSubtract) && (gradientDouble > 0.00))  {
				gradientDouble = gradientDouble - 1;
				gradientString = Double.toString(gradientDouble);
				txtFldGradient.setText(gradientString);
				System.out.println("Test when the time increases or not by 1 = " + gradientDouble);
			}
			else if((e.getSource() == btnAdd) && (gradientDouble == 15.00)) {
				String warnMessage = "The maximum gradient is 15 %";
				String warnTitle = "Invalid user's input";
				JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
			}
			else if((e.getSource() == btnSubtract) && (gradientDouble == 0.00)) {
				String warnMessage = "The minimum gradient is 0 %";
				String warnTitle = "Invalid user's input";
				JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
			}
		}
// Speed Button is pressed the user able to increase or decrease the speed by 1km/h
		if(e.getSource() == btnSpeed) {
			optionButtonState(); // change all option buttons' state to false
			btnSpeedPressed = true;
			System.out.println("button gradien " + btnSpeedPressed);
			buttonEnabledSet(); //  add, subtract, pause buttons are enabled 
		}
// if the speed button was pressed, the user able to use the "+" or "-" buttons to change the speed by +/- 0.2
		if(btnSpeedPressed) {
			if(speedFloat == 1.0) {
				btnStart.setEnabled(true);
			}
			if((e.getSource() == btnAdd)  && (speedFloat < 15.0)) {
				speedFloat = speedFloat + 1;
				speedString = Double.toString(speedFloat);
				System.out.println("Test when the speed increases or not by 0.2 = " + speedFloat);
				txtFldSpeed.setText(speedString);
			}
// "-" decreases the speed by -0.2 
			else if((e.getSource() == btnSubtract) && (speedFloat > 0.0))  {
				speedFloat = speedFloat - 1;
				speedString = Double.toString(speedFloat);	
				txtFldSpeed.setText(speedString);
				System.out.println("Test when the speed increases or not by 0.2 = " + speedFloat);
			}
			else if((e.getSource() == btnAdd) && (speedFloat == 15.0)) {
				String warnMessage = "The maximum speed is 15 km/h";
				String warnTitle = "Invalid user's input";
				JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
			}
			else if((e.getSource() == btnSubtract) && (speedFloat == 0)) {
				String warnMessage = "The minimum speed is 0 km/h";
				String warnTitle = "Invalid user's input";
				JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
			}
		}
// Distance Button is pressed the user able to increase or decrease the distance
		if(e.getSource() == btnDistance) {
			optionButtonState(); // change all option buttons' state to false
			btnDistancePressed = true;
			System.out.println("button Distance " + btnDistancePressed);
			buttonEnabledSet(); //  add, subtract, pause buttons are enabled 
		}
// if the distance button was pressed, the user able to use the "+" or "-" buttons to change the calories by +/- 20
		if(btnDistancePressed) {
			if((e.getSource() == btnAdd)  && (distanceInt < 5000)) {
				distanceInt = distanceInt + 50;
				distanceString = Integer.toString(distanceInt);
				System.out.println("Test when the speed increases or not by 50 = " + distanceInt);
				txtFldDistance.setText(distanceString);
			}
// "-" decreases the calorie by 50
			else if((e.getSource() == btnSubtract) && (distanceInt > 0))  {
				distanceInt = distanceInt - 50;
				distanceString = Integer.toString(distanceInt);
				txtFldDistance.setText(distanceString);
				System.out.println("Test when the speed increases or not by 50 = " + distanceInt);
			}
			else if((e.getSource() == btnAdd) && (distanceInt == 5000)) {
				String warnMessage = "The maximum distance is 5000 meters";
				String warnTitle = "Invalid user's input";
				JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
			}
			else if((e.getSource() == btnSubtract) && (distanceInt == 0)) {
				String warnMessage = "The minimum calories is 0 meter";
				String warnTitle = "Invalid user's input";
				JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
			}
		}
// Calories Button is pressed the user able to increase or decrease the target number by 50
		if(e.getSource() == btnCalories) {
			optionButtonState(); // change all option buttons' state to false
			btnCaloriesPressed = true;
			System.out.println("button gradien " + btnCaloriesPressed);
			buttonEnabledSet(); //  add, subtract, pause buttons are enabled 
		}
// if the calories button was pressed, the user able to use the "+" or "-" buttons to change the calories by +/- 50
			if(btnCaloriesPressed) {
				if((e.getSource() == btnAdd)  && (caloriesInt < 500)) {
					caloriesInt = caloriesInt + 50;
					caloriesString = Integer.toString(caloriesInt);
					txtFldCalories.setText(caloriesString);
				}
// "-" decreases the calorie by 50
				else if((e.getSource() == btnSubtract) && (caloriesInt > 0))  {
					caloriesInt = caloriesInt - 50;
					caloriesString = Integer.toString(caloriesInt);
					caloriesString = Integer.toString(caloriesInt);
					txtFldCalories.setText(caloriesString);
					System.out.println("Test when the speed increases or not by 50 = " + caloriesInt);
				}
				else if((e.getSource() == btnAdd) && (caloriesInt == 500)) {
					String warnMessage = "The maximum calories is 500 cal";
					String warnTitle = "Invalid user's input";
					JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
				}
				else if((e.getSource() == btnSubtract) && (caloriesInt == 0)) {
					String warnMessage = "The minimum calories is 0 cal";
					String warnTitle = "Invalid user's input";
					JOptionPane.showMessageDialog(this, warnMessage, warnTitle, JOptionPane.ERROR_MESSAGE);
				}
			}
	}
	

// method contains time, gradient, speed, calories, distance buttons' state == false 
	public void optionButtonState() {
		btnTimePressed = false;
		btnGradientPressed = false;
		btnSpeedPressed = false;
		btnCaloriesPressed = false;
		btnDistancePressed = false;
	}
	
// method: Set buttons to enabled "true"
	public void buttonEnabledSet() {
		btnAdd.setEnabled(true);
		btnSubtract.setEnabled(true);
		btnPause.setEnabled(true);
	}
// method Set programs buttons to enabled
	public void programButtonSet() {
		btnManual.setEnabled(false);
		btnInterval.setEnabled(false);
		btnFitness.setEnabled(false);
		btnPerformance.setEnabled(false);
	}
// method: reset all buttons 
	public void resetAllButtons() {
		btnManual.setEnabled(true);
		btnPerformance.setEnabled(true);
		btnInterval.setEnabled(true);
		btnFitness.setEnabled(true);
		btnAdd.setEnabled(false);
		btnSubtract.setEnabled(false);
		btnStop.setEnabled(false);
		btnSave.setEnabled(false);
		btnStart.setEnabled(false);
		btnPause.setEnabled(false);
		btnReset.setEnabled(false);
		btnTime.setEnabled(false);
		btnGradient.setEnabled(false);
		btnSpeed.setEnabled(false);
		btnCalories.setEnabled(false);
		btnDistance.setEnabled(false);
		txtFldDistance.setText("-----------------------");
		txtFldSpeed.setText("-----------------------");
		txtFldCalories.setText("-----------------");
		txtFldHeartRate.setText("-----------------");
		txtFldGradient.setText("-----------------");
		txtFldTime.setText("-- / --");
		timerDouble = 0.00;
		timerString = "";
		gradientDouble = 0.00;
		gradientString = "";
		speedFloat = 0.0f;
		speedString = "";
		caloriesInt = 0;
		caloriesString = "";
		distanceInt =0;
		distanceString = "";
		minuteInt = (int)Math.round(timerDouble);
//		tempMinute = minuteInt; 
		timer.stop();
	}
	
	public void startingSettings() {
		btnReset.setEnabled(true);
		btnPause.setEnabled(false);
		btnStart.setEnabled(false);
		btnStop.setEnabled(false);
		btnSave.setEnabled(false);
		btnTime.setEnabled(true);
		btnGradient.setEnabled(true);
		btnSpeed.setEnabled(true);
		btnCalories.setEnabled(true);
		btnDistance.setEnabled(true);
		btnAdd.setEnabled(false);
		btnSubtract.setEnabled(false);
		btnManual.setEnabled(false);
		btnPerformance.setEnabled(false);
		btnInterval.setEnabled(false);
		btnFitness.setEnabled(false);
	}
	
/**
 * Methods are used when the manual button is selected
 */
	public void manualSetStartingPosition() {
		txtFldHeartRate.setText("000");
		txtFldTime.setText("0000");
		txtFldCalories.setText("000");
		txtFldGradient.setText("000");
		txtFldSpeed.setText("00.00");
		txtFldDistance.setText("0000");
	}
/**
* Methods are used when the performance button is selected
*/
	public void performanceSetStartingPosition() {
		timerDouble = 20.00;
		timerString = Double.toString(timerDouble);
		txtFldTime.setText(timerString);
		gradientDouble = 4.00;
		gradientString = Double.toString(gradientDouble);
		txtFldGradient.setText(gradientString);
		gradientString = "";
		speedFloat = 10.0f;
		speedString = Float.toString(speedFloat);
		txtFldSpeed.setText(speedString);
		btnStart.setEnabled(true);
	}
/**
* Methods are used when the interval button is selected
*/
	public void intervalSetStartingPosition() {
		timerDouble = 50.00;
		timerString = Double.toString(timerDouble);
		txtFldTime.setText(timerString);
		gradientDouble = 2.00;
		gradientString = Double.toString(gradientDouble);
		txtFldGradient.setText(gradientString);
		gradientString = "";
		speedFloat = 6.0f;
		speedString = Float.toString(speedFloat);
		txtFldSpeed.setText(speedString);
		btnStart.setEnabled(true);
	}
/**
* Method is used when the fitness button is selected
*/
	public void fitnessSetStartingPosition() {
		timerDouble = 30.00;
		timerString = Double.toString(timerDouble);
		txtFldTime.setText(timerString);
		gradientDouble = 0.00;
		gradientString = Double.toString(gradientDouble);
		txtFldGradient.setText(gradientString);
		gradientString = "";
		speedFloat = 12.0f;
		speedString = Float.toString(speedFloat);
		txtFldSpeed.setText(speedString);
		btnStart.setEnabled(true);
	}
/**
 * Method is used when the start button is pressed
 */
	public void startButtonStarting() {
		btnPause.setEnabled(true);
		btnStop.setEnabled(true);
	}
	
	public void start() {
		counterThread = new Thread();
		
	}
	
	public void run() {
		int	counter = timerDouble.intValue()* 60;
		String newTime = null;
		for(int i=0; i < 1000; i++) {
			newTime = Integer.toString(i);
			System.out.println(newTime);
			txtFldTime.setText(newTime);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
	}
}


