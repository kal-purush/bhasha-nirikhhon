package FinalProject;

import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
//import javax.util.Timer;
//import javax.util.Timer;


/**
 * 
 * 
 * File Name : FinalProject.java
 * Author : Sean Hamilton
 * Date: 03/05/2020
 * Purpose of program: Holds the GUI constructor and all necessary event listeners, buttons, and error handling per project requirements. 
 * 
 *
 */

public class FinalProjectGUI extends JFrame {
	//Height and Width of JFrame
	static final int WIDTH = 800, HEIGHT = 400;
	
	//All necessary JLabels, textareas, and buttons per project requirements.
	JLabel timeShown = new JLabel("Time: ");
	JTextArea timeTextArea = new JTextArea(2, 2);
	JLabel trafficLightOne = new JLabel("Trafflic Light 1: ");
	JTextArea trafficLightOneTextArea = new JTextArea(2, 2);
	JLabel trafficLightTwo = new JLabel("Trafflic Light 2: ");
	JTextArea trafficLightTwoTextArea = new JTextArea(2, 2);
	JLabel trafficLightThree = new JLabel("Trafflic Light 3: ");
	JTextArea trafficLightThreeTextArea = new JTextArea(2, 2);
	JLabel carOne = new JLabel("Car One X & Y Position: ");
	JTextArea carOneTextArea = new JTextArea(2, 2);
	JLabel carTwo = new JLabel("Car Two X & Y Position: ");
	JTextArea carTwoTextArea = new JTextArea(2, 2);
	JLabel carThree = new JLabel("Car Three X & Y Position: ");
	JTextArea carThreeTextArea = new JTextArea(2, 2);
	JButton start = new JButton("Start");
	JButton stop = new JButton("Stop");
	java.util.Timer timer = new java.util.Timer();
	final AtomicInteger value = new AtomicInteger(0);
	public FinalProjectGUI() {
		super("Final Project");
		setFrame(WIDTH, HEIGHT);
		JPanel trafficAnalysisPanel = new JPanel();
		trafficAnalysisPanel.setLayout(new GridLayout(0,2));
		trafficAnalysisPanel.add(timeShown);
		timeTextArea.setEditable(false);
		timeTextArea.setText(String.valueOf(0));
		trafficLightOneTextArea.setEditable(false);
		trafficLightTwoTextArea.setEditable(false);
		trafficLightThreeTextArea.setEditable(false);
		carOneTextArea.setEditable(false);
		carTwoTextArea.setEditable(false);
		carThreeTextArea.setEditable(false);
		trafficAnalysisPanel.add(timeTextArea);
		trafficAnalysisPanel.add(trafficLightOne);
		trafficAnalysisPanel.add(trafficLightOneTextArea);
		trafficAnalysisPanel.add(trafficLightTwo);
		trafficAnalysisPanel.add(trafficLightTwoTextArea);
		trafficAnalysisPanel.add(trafficLightThree);
		trafficAnalysisPanel.add(trafficLightThreeTextArea);
		trafficAnalysisPanel.add(carOne);
		carOne.setText("Car One Traveling at 5mph, Y position = 0, X position = ");
		trafficAnalysisPanel.add(carOneTextArea);
		trafficAnalysisPanel.add(carTwo);
		carTwo.setText("Car One Traveling at 10mph, Y position = 0, X position = ");
		trafficAnalysisPanel.add(carTwoTextArea);
		trafficAnalysisPanel.add(carThree);
		carThree.setText("Car One Traveling at 15mph, Y position = 0, X position = ");
		trafficAnalysisPanel.add(carThreeTextArea);
		trafficAnalysisPanel.add(start);

		

		
		start.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) { 
				
				
				//Separate Thread for timer per requirements.
				Thread timerThread = new Thread() {
					public void run() {
						if(start.getText().equals("Start")) {
							timer = new Timer();
//							Timer Task to simulate pause,start, and resume functionalities. 
							timer.scheduleAtFixedRate(new TimerTask() {
								int count = 0;
								public void run() {
									count++;
									value.incrementAndGet();
									System.out.println(value);
									timeTextArea.setText(new Date().toString());
									
									if(value.get() > 0) {
										trafficLightOneTextArea.setText("GREEN");
										trafficLightTwoTextArea.setText("GREEN");
										trafficLightThreeTextArea.setText("GREEN");
										carOneTextArea.setText(String.valueOf(value.get()+5));
										carTwoTextArea.setText(String.valueOf(value.get()+10));
										carThreeTextArea.setText(String.valueOf(value.get()+15));
									}
									if(value.get() == 9 || value.get() == 19 || value.get() == 29) {
										trafficLightOneTextArea.setText("YELLOW");
										
									}
									if(value.get() == 10 ||value.get() == 11 ||value.get() == 12 ||value.get() == 21 || value.get() == 12 || value.get() == 20 || value.get() == 30 || value.get() == 31 || value.get() == 32) {
										trafficLightOneTextArea.setText("RED");
										carOneTextArea.setText(String.valueOf(0));
										carTwoTextArea.setText(String.valueOf(0));
										carThreeTextArea.setText(String.valueOf(0));
										
									}
									if(value.get() == 14 || value.get() == 29 || value.get() == 44) {
										trafficLightTwoTextArea.setText("YELLOW");
									}
									if(value.get() == 15 ||value.get() == 16 ||value.get() == 17 || value.get() == 30 || value.get() == 45) {
										
										trafficLightTwoTextArea.setText("RED");
										carOneTextArea.setText(String.valueOf(0));
										carTwoTextArea.setText(String.valueOf(0));
										carThreeTextArea.setText(String.valueOf(0));
									}
									if(value.get() == 19 || value.get() == 39 || value.get() == 59) {
										trafficLightThreeTextArea.setText("YELLOW");
										
									}
									if(value.get() == 20 ||value.get() == 21 ||value.get() == 22 || value.get() == 40 || value.get() == 41 || value.get() == 42 || value.get() ==60) {
										trafficLightThreeTextArea.setText("RED");
										carOneTextArea.setText(String.valueOf(0));
										carTwoTextArea.setText(String.valueOf(0));
										carThreeTextArea.setText(String.valueOf(0));
										
									}
									
								}
							}, 0, 1000);
							start.setText("Pause");
						}else if(start.getText().equals("Pause")){
							timer.cancel();
							start.setText("Resume");
						}else if(start.getText().equals("Resume")) {
							timer = new Timer();
							
							timer.scheduleAtFixedRate(new TimerTask() {
								
								public void run() {
									value.set(value.get());
									value.incrementAndGet();
									timeTextArea.setText(new Date().toString());
									if(value.get() > 0) {
										trafficLightOneTextArea.setText("GREEN");
										trafficLightTwoTextArea.setText("GREEN");
										trafficLightThreeTextArea.setText("GREEN");
										carOneTextArea.setText(String.valueOf(value.get()+5));
										carTwoTextArea.setText(String.valueOf(value.get()+10));
										carThreeTextArea.setText(String.valueOf(value.get()+15));
									}
									if(value.get() == 9 || value.get() == 19 || value.get() == 29) {
										trafficLightOneTextArea.setText("YELLOW");
										
									}
									if(value.get() == 10 ||value.get() == 11 ||value.get() == 12 ||value.get() == 21 || value.get() == 12 || value.get() == 20 || value.get() == 30 || value.get() == 31 || value.get() == 32) {
										trafficLightOneTextArea.setText("RED");
										carOneTextArea.setText(String.valueOf(0));
										carTwoTextArea.setText(String.valueOf(0));
										carThreeTextArea.setText(String.valueOf(0));
										
									}
									if(value.get() == 14 || value.get() == 29 || value.get() == 44) {
										trafficLightTwoTextArea.setText("YELLOW");
									}
									if(value.get() == 15 ||value.get() == 16 ||value.get() == 17 || value.get() == 30 || value.get() == 45) {
										
										trafficLightTwoTextArea.setText("RED");
										carOneTextArea.setText(String.valueOf(0));
										carTwoTextArea.setText(String.valueOf(0));
										carThreeTextArea.setText(String.valueOf(0));
									}
									if(value.get() == 19 || value.get() == 39 || value.get() == 59) {
										trafficLightThreeTextArea.setText("YELLOW");
										
									}
									if(value.get() == 20 ||value.get() == 21 ||value.get() == 22 || value.get() == 40 || value.get() == 41 || value.get() == 42 || value.get() ==60) {
										trafficLightThreeTextArea.setText("RED");
										carOneTextArea.setText(String.valueOf(0));
										carTwoTextArea.setText(String.valueOf(0));
										carThreeTextArea.setText(String.valueOf(0));
										
									}
								}
							}, 0, 1000);
							start.setText("Pause");
						}
					}
				};
				timerThread.start();
			}
		});
		//Event listener for stop
		stop.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) { 
				System.exit(0);
			}
		});
		trafficAnalysisPanel.add(stop);	
		
		add(trafficAnalysisPanel);
	}

	public static void main(String[] args) {
		FinalProjectGUI f = new FinalProjectGUI();
		f.display();
	}
	
	public void display() {
	      setVisible(true);
	}
	
	private void setFrame(int width, int height) {
	      setSize(width, height);     
	      setLocationRelativeTo(null);  		
	      setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); 
	}
	

}
 