package gui;
import java.awt.BorderLayout;
import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.filechooser.FileFilter;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.JButton;
import javax.swing.JTextField;
import javax.swing.JFileChooser;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.awt.event.ActionEvent;
import java.awt.Color;



public class fileReader extends JFrame {

	private JPanel contentPane;
	private JTextField openPath;
	private JTextField savePath;
	

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					fileReader frame = new fileReader();
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
	public fileReader() {
		
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 516, 310);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);
		
		JButton btnOpenFile = new JButton("Select");	
		btnOpenFile.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JFileChooser fileOpenChooser = new JFileChooser();			
				FileFilter filterAnBFiles = new FileNameExtensionFilter("AnB and AnBx files", "AnB", "AnBx");
				fileOpenChooser.setFileFilter(filterAnBFiles);
				fileOpenChooser.setAcceptAllFileFilterUsed(false);
				
				if (fileOpenChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
					openPath.setText(fileOpenChooser.getSelectedFile().toString());
				} else {
					if(openPath.getText()== null || openPath.getText()== "")
						openPath.setText("");
				}				
			}
		});
		btnOpenFile.setBounds(358, 32, 103, 23);
		contentPane.add(btnOpenFile);
		
		JButton btnSaveFile = new JButton("Destination");
		btnSaveFile.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JFileChooser fileOpenChooser = new JFileChooser();			
				FileFilter filterAnBFiles = new FileNameExtensionFilter("AnB and AnBx files", "AnB", "AnBx");
				fileOpenChooser.setFileFilter(filterAnBFiles);
				fileOpenChooser.setAcceptAllFileFilterUsed(false);
				
				if (fileOpenChooser.showSaveDialog(null) == JFileChooser.APPROVE_OPTION) {
					savePath.setText(fileOpenChooser.getSelectedFile().toString()+".vlsm");
				} else {
					if(savePath.getText()== null || savePath.getText()== "")
						savePath.setText("");
				}
			}
		});
		btnSaveFile.setBounds(358, 76, 103, 23);
		contentPane.add(btnSaveFile);
		
		JButton btnAnalize = new JButton("Analize Input File");
		btnAnalize.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(openPath.getText() != null && openPath.getText() != "")
				{
					System.out.println("click analized");
					
					//AtnlrHandler basic = new AtnlrHandler();
					
				}
			}
		});
		btnAnalize.setBounds(102, 158, 131, 23);
		contentPane.add(btnAnalize);
		
		JButton btnCreateFile = new JButton("Create Vlsm");
		btnCreateFile.setBounds(309, 158, 89, 23);
		contentPane.add(btnCreateFile);
		
		openPath = new JTextField();
		openPath.setBackground(new Color(255, 255, 255));
		openPath.setEditable(false);
		openPath.setBounds(21, 33, 327, 20);
		contentPane.add(openPath);
		openPath.setColumns(10);
		
		savePath = new JTextField();
		savePath.setBackground(new Color(255, 255, 255));
		savePath.setEditable(false);
		savePath.setBounds(21, 77, 327, 20);
		contentPane.add(savePath);
		savePath.setColumns(10);
		
		
	}
}