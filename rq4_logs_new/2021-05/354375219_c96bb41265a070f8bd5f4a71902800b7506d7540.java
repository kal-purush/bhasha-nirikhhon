
import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import javax.swing.JLabel;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.Color;
import javax.swing.SwingConstants;
import javax.swing.JButton;

public class ABAbox extends JFrame {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JPanel contentPane;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					ABAbox frame = new ABAbox();
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
	public ABAbox() {
		setTitle("ABA Window");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 710, 508);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);
		
		JLabel AdminrightsLabel = new JLabel("Administrator rights");
		AdminrightsLabel.setHorizontalAlignment(SwingConstants.LEFT);
		AdminrightsLabel.setForeground(new Color(255, 0, 0));
		AdminrightsLabel.setBackground(new Color(250, 128, 114));
		AdminrightsLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
		AdminrightsLabel.setBounds(30, 21, 147, 14);
		contentPane.add(AdminrightsLabel);
		
		JLabel optionALabel = new JLabel("PLEASE CHOOSE AN OPTION:");
		optionALabel.setFont(new Font("Trebuchet MS", Font.BOLD, 19));
		optionALabel.setBounds(30, 67, 313, 33);
		contentPane.add(optionALabel);
		
		JButton addContractsButton = new JButton("Add/Edit contracts");
		addContractsButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				setVisible(false);
				Contractsbox contractsBox = new Contractsbox();
				contractsBox.setVisible(true);
			}
		});
		addContractsButton.setFont(new Font("Tahoma", Font.ITALIC, 17));
		addContractsButton.setBackground(new Color(135, 206, 235));
		addContractsButton.setBounds(47, 246, 212, 47);
		contentPane.add(addContractsButton);
		
		JButton viewDetailsButton = new JButton("View details");
		viewDetailsButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Viewdetailsbox viewDetailsbox = new Viewdetailsbox();
				viewDetailsbox.setVisible(true);
				setVisible(false);
			}
		});
		viewDetailsButton.setBackground(new Color(135, 206, 235));
		viewDetailsButton.setFont(new Font("Tahoma", Font.ITALIC, 17));
		viewDetailsButton.setBounds(47, 348, 212, 47);
		contentPane.add(viewDetailsButton);
		
		JButton viewRequestsButton = new JButton("View requests");
		viewRequestsButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Viewrequestsbox viewRequestsbox = new Viewrequestsbox();
				viewRequestsbox.setVisible(true);
				setVisible(false);
			}
		});
		viewRequestsButton.setFont(new Font("Tahoma", Font.ITALIC, 17));
		viewRequestsButton.setBackground(new Color(135, 206, 235));
		viewRequestsButton.setBounds(405, 137, 212, 47);
		contentPane.add(viewRequestsButton);
		
		JButton accountingButton = new JButton("Accounting");
		accountingButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				setVisible(false);
				Accountingbox accBox = new Accountingbox();
				accBox.setVisible(true);
			}
		});
		accountingButton.setFont(new Font("Tahoma", Font.ITALIC, 17));
		accountingButton.setBackground(new Color(135, 206, 235));
		accountingButton.setBounds(405, 246, 212, 47);
		contentPane.add(accountingButton);
		
		//BEGIN ACTION ON LOG OUT BUTTON
		
		JButton LogoutButton = new JButton("Log out");
		LogoutButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				setVisible(false);
				Loginbox window = new Loginbox();
				window.LoginFrame.setVisible(true);
			}
		});
		LogoutButton.setFont(new Font("Tahoma", Font.ITALIC, 17));
		LogoutButton.setBackground(new Color(135, 206, 235));
		LogoutButton.setBounds(405, 348, 212, 47);
		contentPane.add(LogoutButton);
		
		JButton addEmployeeButton = new JButton("Add/Edit employees");
		addEmployeeButton.setFont(new Font("Tahoma", Font.ITALIC, 17));
		addEmployeeButton.setBackground(new Color(135, 206, 235));
		
		//BEGIN ACTION ON ADD EMPLOYEES BUTTON
		
				addEmployeeButton.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						
						Employeesbox employeesBox = new Employeesbox();
						employeesBox.setVisible(true);
						setVisible(false);
						
					}
				});
		addEmployeeButton.setBounds(47, 137, 212, 47);
		contentPane.add(addEmployeeButton);
	}
}