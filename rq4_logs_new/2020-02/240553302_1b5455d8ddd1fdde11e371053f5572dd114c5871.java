package Restaurant;

import java.awt.EventQueue;

import javax.swing.JFrame;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JLabel;
import java.awt.Font;
import java.awt.Color;
import java.awt.FlowLayout;
import javax.swing.JPanel;
import javax.swing.border.BevelBorder;
import javax.swing.JComboBox;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JSeparator;
import java.awt.Rectangle;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.JButton;
import javax.swing.border.EtchedBorder;
import javax.swing.JCheckBox;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.swing.border.LineBorder;
import javax.swing.JTextPane;

import sun.java2d.pipe.SpanShapeRenderer.Simple;
import javax.swing.JTextArea;
import java.io.*;

public class Restaurant {
	public int token=1,order_id=0;
	public double t_vburger=0,t_burrito=0,t_cburger=0,t_jburger=0,t_csandwich=0,t_sdrink=0,t_mshake=0;
	public double tax,sub_total,service_tax=0,total;
	private JFrame frame;
	private JTextField veg_burger;
	private JTextField burrito;
	private JTextField chicken_burger;
	private JTextField jumbo_burger;
	private JTextField cheese_sandwich;
	private JTextField soft_drink;
	private JTextField milk_shake;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					Restaurant window = new Restaurant();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public Restaurant() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.getContentPane().setFont(new Font("Calibri", Font.PLAIN, 11));
		frame.getContentPane().setBounds(new Rectangle(0, 0, 50, 50));
		frame.setBounds(0, 0, 1280, 720);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);
		
		JLabel lblNewLabel = new JLabel("SAARR Restaurant");
		lblNewLabel.setBounds(365, 2, 605, 84);
		lblNewLabel.setBackground(Color.WHITE);
		lblNewLabel.setForeground(Color.ORANGE);
		lblNewLabel.setFont(new Font("Calibri", Font.PLAIN, 77));
		frame.getContentPane().add(lblNewLabel);
		
		JPanel panel = new JPanel();
		panel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		panel.setBounds(22, 97, 379, 456);
		frame.getContentPane().add(panel);
		panel.setLayout(null);
		
		JLabel lblNewLabel_2 = new JLabel("Menu");
		lblNewLabel_2.setForeground(Color.RED);
		lblNewLabel_2.setFont(new Font("Calibri", Font.PLAIN, 38));
		lblNewLabel_2.setBounds(136, 11, 107, 36);
		panel.add(lblNewLabel_2);
		
		final JComboBox comboBox = new JComboBox();
		comboBox.setForeground(new Color(0, 0, 255));
		comboBox.setFont(new Font("Calibri", Font.PLAIN, 21));
		comboBox.setModel(new DefaultComboBoxModel(new String[] {"Select a drink", "Coke(Regular)", "Coke(Diet) ", "Pepsi(Regular)", "Pepsi(Diet)", "Crush Pineapple", "Crush Peach", "Canada Dry", "Fresca"}));
		comboBox.setBounds(20, 308, 151, 20);
		panel.add(comboBox);
		
		JSeparator separator = new JSeparator();
		separator.setBounds(new Rectangle(0, 0, 20, 20));
		separator.setForeground(new Color(255, 0, 0));
		separator.setBounds(10, 253, 359, 2);
		panel.add(separator);
		
		JLabel lblNewLabel_4 = new JLabel("$  7.00");
		lblNewLabel_4.setForeground(new Color(0, 0, 255));
		lblNewLabel_4.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblNewLabel_4.setBounds(173, 64, 70, 14);
		panel.add(lblNewLabel_4);
		
		JLabel label_4 = new JLabel("$  6.50");
		label_4.setForeground(new Color(0, 0, 255));
		label_4.setFont(new Font("Calibri", Font.PLAIN, 21));
		label_4.setBounds(173, 101, 70, 14);
		panel.add(label_4);
		
		JLabel label_5 = new JLabel("$  5.25");
		label_5.setForeground(new Color(0, 0, 255));
		label_5.setFont(new Font("Calibri", Font.PLAIN, 21));
		label_5.setBounds(173, 138, 70, 14);
		panel.add(label_5);
		
		JLabel label_6 = new JLabel("$ 10.00");
		label_6.setForeground(new Color(0, 0, 255));
		label_6.setFont(new Font("Calibri", Font.PLAIN, 21));
		label_6.setBounds(173, 178, 70, 14);
		panel.add(label_6);
		
		JLabel label_7 = new JLabel("$  4.65");
		label_7.setForeground(new Color(0, 0, 255));
		label_7.setFont(new Font("Calibri", Font.PLAIN, 21));
		label_7.setBounds(173, 220, 70, 14);
		panel.add(label_7);
		
		JLabel lblNewLabel_5 = new JLabel("$  2.00");
		lblNewLabel_5.setForeground(new Color(0, 0, 255));
		lblNewLabel_5.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblNewLabel_5.setBounds(173, 277, 62, 15);
		panel.add(lblNewLabel_5);
		
		JLabel lblDrink = new JLabel("Soft Drink");
		lblDrink.setForeground(new Color(0, 0, 255));
		lblDrink.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblDrink.setBounds(20, 271, 97, 26);
		panel.add(lblDrink);
		
		JLabel lblMilkShake = new JLabel("Milk Shake");
		lblMilkShake.setForeground(Color.BLUE);
		lblMilkShake.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblMilkShake.setBounds(20, 354, 97, 26);
		panel.add(lblMilkShake);
		
		JLabel label_8 = new JLabel("$  3.00");
		label_8.setForeground(Color.BLUE);
		label_8.setFont(new Font("Calibri", Font.PLAIN, 21));
		label_8.setBounds(173, 360, 62, 15);
		panel.add(label_8);
		
		final JComboBox comboBox_1 = new JComboBox();
		comboBox_1.setModel(new DefaultComboBoxModel(new String[] {"Select a milk shake", "Chocolate", "Strawberry", "Vanilla", "Peach", "Plum", "Butter Scotch"}));
		comboBox_1.setForeground(Color.BLUE);
		comboBox_1.setFont(new Font("Calibri", Font.PLAIN, 21));
		comboBox_1.setBounds(20, 400, 190, 20);
		panel.add(comboBox_1);
		
		veg_burger = new JTextField();
		veg_burger.setHorizontalAlignment(SwingConstants.RIGHT);
		veg_burger.setFont(new Font("Algerian", Font.PLAIN, 21));
		veg_burger.setBounds(265, 60, 86, 20);
		panel.add(veg_burger);
		veg_burger.setColumns(10);
		
		burrito = new JTextField();
		burrito.setHorizontalAlignment(SwingConstants.RIGHT);
		burrito.setFont(new Font("Algerian", Font.PLAIN, 21));
		burrito.setColumns(10);
		burrito.setBounds(265, 97, 86, 20);
		panel.add(burrito);
		
		chicken_burger = new JTextField();
		chicken_burger.setHorizontalAlignment(SwingConstants.RIGHT);
		chicken_burger.setFont(new Font("Algerian", Font.PLAIN, 21));
		chicken_burger.setColumns(10);
		chicken_burger.setBounds(265, 134, 86, 20);
		panel.add(chicken_burger);
		
		jumbo_burger = new JTextField();
		jumbo_burger.setHorizontalAlignment(SwingConstants.RIGHT);
		jumbo_burger.setFont(new Font("Algerian", Font.PLAIN, 21));
		jumbo_burger.setColumns(10);
		jumbo_burger.setBounds(265, 174, 86, 20);
		panel.add(jumbo_burger);
		
		cheese_sandwich = new JTextField();
		cheese_sandwich.setHorizontalAlignment(SwingConstants.RIGHT);
		cheese_sandwich.setFont(new Font("Algerian", Font.PLAIN, 21));
		cheese_sandwich.setColumns(10);
		cheese_sandwich.setBounds(265, 216, 86, 20);
		panel.add(cheese_sandwich);
		
		soft_drink = new JTextField();
		soft_drink.setHorizontalAlignment(SwingConstants.RIGHT);
		soft_drink.setFont(new Font("Algerian", Font.PLAIN, 21));
		soft_drink.setColumns(10);
		soft_drink.setBounds(265, 292, 86, 20);
		panel.add(soft_drink);
		
		milk_shake = new JTextField();
		milk_shake.setHorizontalAlignment(SwingConstants.RIGHT);
		milk_shake.setFont(new Font("Algerian", Font.PLAIN, 21));
		milk_shake.setColumns(10);
		milk_shake.setBounds(265, 381, 86, 20);
		panel.add(milk_shake);
		
		JLabel lblVegBurger = new JLabel("Veg Burger");
		lblVegBurger.setForeground(Color.BLUE);
		lblVegBurger.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblVegBurger.setBounds(10, 57, 107, 26);
		panel.add(lblVegBurger);
		
		JLabel lblBurrito = new JLabel("Burrito");
		lblBurrito.setForeground(Color.BLUE);
		lblBurrito.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblBurrito.setBounds(10, 94, 107, 26);
		panel.add(lblBurrito);
		
		JLabel lblChickenBurger = new JLabel("Chicken Burger");
		lblChickenBurger.setForeground(Color.BLUE);
		lblChickenBurger.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblChickenBurger.setBounds(10, 131, 140, 26);
		panel.add(lblChickenBurger);
		
		JLabel lblJumboBurger = new JLabel("Jumbo Burger");
		lblJumboBurger.setForeground(Color.BLUE);
		lblJumboBurger.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblJumboBurger.setBounds(10, 171, 129, 26);
		panel.add(lblJumboBurger);
		
		JLabel lblCheeseSandwich = new JLabel("Cheese Sandwich");
		lblCheeseSandwich.setForeground(Color.BLUE);
		lblCheeseSandwich.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblCheeseSandwich.setBounds(10, 214, 161, 26);
		panel.add(lblCheeseSandwich);
		
		JPanel panel_1 = new JPanel();
		panel_1.setBounds(new Rectangle(0, 0, 50, 50));
		panel_1.setBorder(new EtchedBorder(EtchedBorder.LOWERED, null, null));
		panel_1.setBounds(443, 351, 238, 202);
		frame.getContentPane().add(panel_1);
		panel_1.setLayout(null);
		
		JLabel lblTax = new JLabel("Tax");
		lblTax.setForeground(Color.BLUE);
		lblTax.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblTax.setBounds(10, 11, 81, 26);
		panel_1.add(lblTax);
		
		JLabel lblServiceTax = new JLabel("Service Tax");
		lblServiceTax.setForeground(Color.BLUE);
		lblServiceTax.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblServiceTax.setBounds(10, 58, 115, 26);
		panel_1.add(lblServiceTax);
		
		final JLabel lblSubTotal = new JLabel("Sub Total");
		lblSubTotal.setForeground(Color.BLUE);
		lblSubTotal.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblSubTotal.setBounds(10, 105, 115, 26);
		panel_1.add(lblSubTotal);
		
		JLabel lblTotal = new JLabel("Total");
		lblTotal.setForeground(Color.BLUE);
		lblTotal.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblTotal.setBounds(10, 153, 81, 26);
		panel_1.add(lblTotal);
		
		final JLabel lbl_tax = new JLabel("");
		lbl_tax.setForeground(new Color(255, 0, 0));
		lbl_tax.setFont(new Font("Algerian", Font.PLAIN, 21));
		lbl_tax.setBounds(147, 11, 81, 26);
		panel_1.add(lbl_tax);
		
		final JLabel lbl_service_tax = new JLabel("");
		lbl_service_tax.setForeground(Color.RED);
		lbl_service_tax.setFont(new Font("Algerian", Font.PLAIN, 21));
		lbl_service_tax.setBounds(147, 58, 81, 26);
		panel_1.add(lbl_service_tax);
		
		final JLabel lbl_sub_total = new JLabel("");
		lbl_sub_total.setLabelFor(lblSubTotal);
		lbl_sub_total.setForeground(Color.RED);
		lbl_sub_total.setFont(new Font("Algerian", Font.PLAIN, 21));
		lbl_sub_total.setBounds(147, 105, 81, 26);
		panel_1.add(lbl_sub_total);
		
		final JLabel lbl_total = new JLabel("");
		lbl_total.setForeground(Color.RED);
		lbl_total.setFont(new Font("Algerian", Font.PLAIN, 21));
		lbl_total.setBounds(147, 153, 81, 26);
		panel_1.add(lbl_total);
		
		JPanel panel_2 = new JPanel();
		panel_2.setBounds(new Rectangle(0, 0, 50, 50));
		panel_2.setBorder(new EtchedBorder(EtchedBorder.LOWERED, null, null));
		panel_2.setBounds(443, 100, 238, 188);
		frame.getContentPane().add(panel_2);
		panel_2.setLayout(null);
		
		JLabel lblNewLabel_6 = new JLabel("Your Token ");
		lblNewLabel_6.setForeground(new Color(0, 0, 255));
		lblNewLabel_6.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblNewLabel_6.setVerticalAlignment(SwingConstants.TOP);
		lblNewLabel_6.setBounds(63, 11, 118, 33);
		panel_2.add(lblNewLabel_6);
		
		JLabel lblNumberIs = new JLabel("Number is... ");
		lblNumberIs.setVerticalAlignment(SwingConstants.TOP);
		lblNumberIs.setForeground(Color.BLUE);
		lblNumberIs.setFont(new Font("Calibri", Font.PLAIN, 21));
		lblNumberIs.setBounds(63, 55, 118, 33);
		panel_2.add(lblNumberIs);
		
		final JLabel token_no = new JLabel("");
		token_no.setForeground(new Color(255, 0, 0));
		token_no.setFont(new Font("ArabDances", Font.PLAIN, 39));
		token_no.setBounds(91, 99, 48, 45);
		panel_2.add(token_no);
		
		final JCheckBox chckbxNewCheckBox = new JCheckBox("Service Tax");
		chckbxNewCheckBox.setForeground(new Color(255, 0, 0));
		chckbxNewCheckBox.setFont(new Font("Calibri", Font.PLAIN, 21));
		chckbxNewCheckBox.setBounds(487, 308, 147, 23);
		frame.getContentPane().add(chckbxNewCheckBox);
		
		JPanel panel_3 = new JPanel();
		panel_3.setBorder(new LineBorder(new Color(0, 0, 0), 3, true));
		panel_3.setBounds(754, 100, 351, 453);
		frame.getContentPane().add(panel_3);
		panel_3.setLayout(null);
		
		final JTextArea textReceipt = new JTextArea();
		textReceipt.setBounds(10, 11, 331, 431);
		panel_3.add(textReceipt);
		
		JButton btnExit = new JButton("Exit");
		btnExit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				System.exit(0);
			}
		});
		btnExit.setForeground(Color.RED);
		btnExit.setFont(new Font("Calibri", Font.PLAIN, 21));
		btnExit.setBounds(754, 611, 119, 31);
		frame.getContentPane().add(btnExit);
		
		JButton btnNewButton = new JButton("Dine in");
		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				order_id++;
				double i_vburger=Double.parseDouble(veg_burger.getText());
				double rate_vburger=7.00;
				t_vburger=i_vburger*rate_vburger;
				sub_total=t_vburger;
				String d_vburger=Double.toString(sub_total);
				lbl_sub_total.setText(d_vburger);
				
				double i_burrito=Double.parseDouble(burrito.getText());
				double rate_burrito=6.50;
				t_burrito=i_burrito*rate_burrito;
				sub_total=t_burrito+t_vburger;
				String d_burrito=Double.toString(sub_total);
				lbl_sub_total.setText(d_burrito);
				
				double i_cburger=Double.parseDouble(chicken_burger.getText());
				double rate_cburger=5.25;
				t_cburger=i_cburger*rate_cburger;
				sub_total=t_cburger+t_burrito+t_vburger;
				String d_cburger=Double.toString(sub_total);
				lbl_sub_total.setText(d_cburger);
				
				double i_jburger=Double.parseDouble(jumbo_burger.getText());
				double rate_jburger=10.00;
				t_jburger=i_jburger*rate_jburger;
				sub_total=t_jburger+t_cburger+t_burrito+t_vburger;
				String d_jburger=Double.toString(sub_total);
				lbl_sub_total.setText(d_jburger);
				
				double i_csandwich=Double.parseDouble(cheese_sandwich.getText());
				double rate_csandwich=4.65;
				t_csandwich=i_csandwich*rate_csandwich;
				sub_total=t_csandwich+t_jburger+t_cburger+t_burrito+t_vburger;
				String d_csandwich=Double.toString(sub_total);
				lbl_sub_total.setText(d_csandwich);
				

				double i_sdrink=Double.parseDouble(soft_drink.getText());
				double rate_sdrink=2.00;
				t_sdrink=i_sdrink*rate_sdrink;
				sub_total=t_sdrink+t_csandwich+t_jburger+t_cburger+t_burrito+t_vburger;
				String d_sdrink=Double.toString(sub_total);
				lbl_sub_total.setText(d_sdrink);
				

				double i_mshake=Double.parseDouble(milk_shake.getText());
				double rate_mshake=3.00;
				t_mshake=i_mshake*rate_mshake;
				sub_total=t_mshake+t_sdrink+t_csandwich+t_jburger+t_cburger+t_burrito+t_vburger;
				String d_mshake=Double.toString(sub_total);
				lbl_sub_total.setText(d_mshake);
				
				tax= sub_total*0.13;
				tax=Math.round(tax*100.0)/100.0;
				String d_tax=Double.toString(tax);
				lbl_tax.setText(d_tax);
				
				service_tax=sub_total*0.10;
				service_tax=Math.round(service_tax*100.0)/100.0;
				String d_service_tax=Double.toString(service_tax);
				if(chckbxNewCheckBox.isSelected())
				{	
				lbl_service_tax.setText(d_service_tax);
				}
				total=sub_total+tax+service_tax;
				total=Math.round(total*100.0)/100.0;
				String d_total=Double.toString(total);
				//String d_total=String.format("%.2f",total);
				lbl_total.setText(d_total);
				
				
				
			}
		});
		btnNewButton.setForeground(new Color(255, 0, 0));
		btnNewButton.setFont(new Font("Calibri", Font.PLAIN, 21));
		btnNewButton.setBounds(42, 611, 119, 31);
		frame.getContentPane().add(btnNewButton);
		
		JButton btnTakeOut = new JButton("Take out");
		btnTakeOut.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				order_id++;
				double i_vburger=Double.parseDouble(veg_burger.getText());
				double rate_vburger=7.00;
				t_vburger=i_vburger*rate_vburger;
				sub_total=t_vburger;
				String d_vburger=Double.toString(sub_total);
				lbl_sub_total.setText(d_vburger);
				
				double i_burrito=Double.parseDouble(burrito.getText());
				double rate_burrito=6.50;
				t_burrito=i_burrito*rate_burrito;
				sub_total=t_burrito+t_vburger;
				String d_burrito=Double.toString(sub_total);
				lbl_sub_total.setText(d_burrito);
				
				double i_cburger=Double.parseDouble(chicken_burger.getText());
				double rate_cburger=5.25;
				t_cburger=i_cburger*rate_cburger;
				sub_total=t_cburger+t_burrito+t_vburger;
				String d_cburger=Double.toString(sub_total);
				lbl_sub_total.setText(d_cburger);
				
				double i_jburger=Double.parseDouble(jumbo_burger.getText());
				double rate_jburger=10.00;
				t_jburger=i_jburger*rate_jburger;
				sub_total=t_jburger+t_cburger+t_burrito+t_vburger;
				String d_jburger=Double.toString(sub_total);
				lbl_sub_total.setText(d_jburger);
				
				double i_csandwich=Double.parseDouble(cheese_sandwich.getText());
				double rate_csandwich=4.65;
				t_csandwich=i_csandwich*rate_csandwich;
				sub_total=t_csandwich+t_jburger+t_cburger+t_burrito+t_vburger;
				String d_csandwich=Double.toString(sub_total);
				lbl_sub_total.setText(d_csandwich);
				

				double i_sdrink=Double.parseDouble(soft_drink.getText());
				double rate_sdrink=2.00;
				t_sdrink=i_sdrink*rate_sdrink;
				sub_total=t_sdrink+t_csandwich+t_jburger+t_cburger+t_burrito+t_vburger;
				String d_sdrink=Double.toString(sub_total);
				lbl_sub_total.setText(d_sdrink);
				

				double i_mshake=Double.parseDouble(milk_shake.getText());
				double rate_mshake=3.00;
				t_mshake=i_mshake*rate_mshake;
				sub_total=t_mshake+t_sdrink+t_csandwich+t_jburger+t_cburger+t_burrito+t_vburger;
				String d_mshake=Double.toString(sub_total);
				lbl_sub_total.setText(d_mshake);
				
				tax= sub_total*0.13;
				tax=Math.round(tax*100.0)/100.0;
				String d_tax=Double.toString(tax);
				lbl_tax.setText(d_tax);
				
				total=sub_total+tax;
				total=Math.round(total*100.0)/100.0;
				String d_total=Double.toString(total);
				lbl_total.setText(d_total);
				
				
				String Token=Integer.toString(token);
				token++;
				token_no.setText(Token);
				
				
			}
		});
		btnTakeOut.setForeground(Color.RED);
		btnTakeOut.setFont(new Font("Calibri", Font.PLAIN, 21));
		btnTakeOut.setBounds(254, 611, 147, 31);
		frame.getContentPane().add(btnTakeOut);
		
		JButton btnReset = new JButton("Reset");
		btnReset.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				veg_burger.setText(null);
				burrito.setText(null);
				chicken_burger.setText(null);
				jumbo_burger.setText(null);
				cheese_sandwich.setText(null);
				soft_drink.setText(null);
				milk_shake.setText(null);
				lbl_service_tax.setText(null);
				lbl_sub_total.setText(null);
				lbl_tax.setText(null);
				lbl_total.setText(null);
				token_no.setText(null);
				comboBox.setSelectedItem("Select a drink");
				textReceipt.setText(null);
				comboBox_1.setSelectedItem("Select a milk shake");
				chckbxNewCheckBox.setSelected(false);
				
			}
		});
		btnReset.setForeground(Color.RED);
		btnReset.setFont(new Font("Calibri", Font.PLAIN, 21));
		btnReset.setBounds(506, 611, 119, 31);
		frame.getContentPane().add(btnReset);
		
		JButton btnGenerateReceipt = new JButton("Generate Receipt");
		btnGenerateReceipt.setHorizontalAlignment(SwingConstants.LEFT);
		btnGenerateReceipt.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				textReceipt.setEnabled(true);
				textReceipt.setText("\t     SAARR Restaurant\n"+
				"\t              Invoice\n"+
				"Order ID:"+order_id+"\tDate:"+java.time.LocalDate.now()+
				"  Time:"+java.time.LocalTime.now()+"\n");
				boolean f1=false,f2=false,f3=false,f4=false,f5=false,f6=false,f7=false;
				int item1=Integer.parseInt(veg_burger.getText());
				if(item1!=0)f1=true;
				int item2=Integer.parseInt(burrito.getText());
				if(item2!=0)f2=true;
				int item3=Integer.parseInt(chicken_burger.getText());	
				if(item3!=0)f3=true;
				int item4=Integer.parseInt(jumbo_burger.getText());
				if(item4!=0)f4=true;
				int item5=Integer.parseInt(cheese_sandwich.getText());
				if(item5!=0)f5=true;
				int item6=Integer.parseInt(soft_drink.getText());
				if(item6!=0)f6=true;
				int item7=Integer.parseInt(milk_shake.getText());
				if(item7!=0)f7=true;
				while(f1!=false||f2!=false||f3!=false||f4!=false||f5!=false||f6!=false||f7!=false)
				{
					if(f1==true)
					{
				      textReceipt.append("\nVeg burger                   "+item1+"\t"+t_vburger);
				      f1=false;
					}
					if(f2==true)
					{
						textReceipt.append("\nBurrito                           "+item2+"   \t"+t_burrito);
					    f2=false;
					}
				
					if(f3==true)
					{
						textReceipt.append("\nChicken burger           "+item3+"\t"+t_cburger);
					    f3=false;
					}
					if(f4==true)
					{
						textReceipt.append("\nJumbo Burger             "+item4+"\t"+t_jburger);
					    f4=false;
					}
					if(f5==true)
					{
						textReceipt.append("\nCheese Sandwich      "+item5+"\t"+t_csandwich);
					    f5=false;
					}
					if(f6==true)
					{
						textReceipt.append("\nSoft Drink                      "+item6+"\t"+t_sdrink);
					    f6=false;
					}
					if(f7==true)
					{
						textReceipt.append("\nMilk Shake                    "+item7+"\t"+t_mshake);
					    f7=false;
					}
				
				}
				textReceipt.append("\n----------------------------------------------------------\n");
				textReceipt.append(" Sub-total\t"+sub_total);
				textReceipt.append("\n Tax\t"+tax);
				textReceipt.append("\n Service tax\t"+service_tax);
				textReceipt.append("\n Total\t$ "+total);
				
				try {
					File f=new File("one.txt");
					if(!f.exists())
					{
					  PrintWriter outputfile= new PrintWriter("D:/eclipse/Java Programming/Login/one.txt");
					  outputfile.print(java.time.LocalDate.now());
					  outputfile.println("\t"+java.time.LocalTime.now());
					  outputfile.println("Order ID:"+order_id+"  Total:"+total);
					  outputfile.close();
					}
					else
					{
						FileWriter outputfile= new FileWriter("D:/eclipse/Java Programming/Login/one.txt",true);
						PrintWriter fw=new PrintWriter(outputfile);
						fw.print(java.time.LocalDate.now());
						fw.println("\t"+java.time.LocalTime.now());
						fw.println("Order ID:"+order_id+"  Total:"+total);
						fw.close();  
						outputfile.close();
						
					}
					
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		});
		btnGenerateReceipt.setForeground(Color.RED);
		btnGenerateReceipt.setFont(new Font("Calibri", Font.PLAIN, 21));
		btnGenerateReceipt.setBounds(986, 611, 197, 31);
		frame.getContentPane().add(btnGenerateReceipt);
	}
}