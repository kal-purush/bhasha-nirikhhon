package gui;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JPanel;
import javax.swing.JTextField;

import logic.KPSmartController;
import java.awt.BorderLayout;

import javax.swing.JButton;
import javax.swing.JLabel;
import java.awt.Font;
import javax.swing.SwingConstants;

import java.awt.Color;
import java.awt.GridLayout;

public class HomeScreenPanel extends JPanel implements ActionListener {

	private JPanel contentPane;
	private JTextField txtLoading;
	private KPSmartController controller;
	private JTextField txtLoading_1;
	/**
	 * Create the panel.
	 */
	public HomeScreenPanel(KPSmartController controller) {
		setLayout(new BorderLayout(0, 0));
		
		JLabel lblHome = new JLabel("Home");
		lblHome.setHorizontalAlignment(SwingConstants.CENTER);
		lblHome.setFont(new Font("Lucida Grande", Font.BOLD, 24));
		add(lblHome, BorderLayout.NORTH);
		
		txtLoading_1 = new JTextField();
		txtLoading_1.setForeground(Color.LIGHT_GRAY);
		txtLoading_1.setText("Loading...");
		add(txtLoading_1, BorderLayout.SOUTH);
		txtLoading_1.setColumns(10);
		
		JPanel panel = new JPanel();
		add(panel, BorderLayout.CENTER);
		panel.setLayout(new GridLayout(1, 0, 0, 0));
		addButtonsToPanel(panel);

	}
	
	public void addButtonsToPanel(JPanel panel){
		JButton newOrderButton = new JButton("New Order");
		panel.add(newOrderButton);
		newOrderButton.addActionListener(this);
		
		JButton newRouteButton = new JButton("New Route");
		panel.add(newRouteButton);
		newRouteButton.addActionListener(this);

		JButton discontinueRouteButton = new JButton("Discontinue Route");
		panel.add(discontinueRouteButton);
		discontinueRouteButton.addActionListener(this);

		JButton updatePriceButton = new JButton("Update Pricing");
		panel.add(updatePriceButton);
		updatePriceButton.addActionListener(this);

		JButton createNewUserButton = new JButton("Create New User");
		panel.add(createNewUserButton);
		createNewUserButton.addActionListener(this);

		JButton viewBusinessFiguresButton = new JButton("View Business Figures");
		panel.add(viewBusinessFiguresButton);
		viewBusinessFiguresButton.addActionListener(this);
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		//change KPSmartFrame focus to focus of button click
	}

}