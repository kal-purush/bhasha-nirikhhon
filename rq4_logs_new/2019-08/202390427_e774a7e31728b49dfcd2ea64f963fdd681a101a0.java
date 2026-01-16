package airbnb;

import java.awt.EventQueue;
import java.awt.GridLayout;
import java.awt.Image;
import java.awt.Label;
import java.awt.event.*;
import java.awt.image.BufferedImage;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.*;

import java.util.*;
import java.util.Date;
import java.net.URL;
import java.io.IOException;
import javax.imageio.ImageIO;

public class AirbnbInterface extends JFrame {
	private final static String driverUrl = "oracle.jdbc.driver.OracleDriver";
	private final static String DB_URL= "jdbc:oracle:thin:@cs322-db.epfl.ch:1521:ORCLCDB";
	private final static String DB_USER = "C##DB2019_G24";
	private final static String DB_PASSWORD = "DB2019_G24";
	private static final long serialVersionUID = 1L;
	
	public Container cp;
	public JPanel mainPanel;
	public JPanel advancePanel;
	public JPanel operationPanel;
	public JPanel infoPanel;
	public JPanel selectionPanel;
	public JPanel parameterPanel;
	public JPanel presentPanel;
	public JPanel resultPanel;
	public JScrollPane resultScroller;
	public JScrollPane desScroller;
	public JScrollPane presentScroller;
	private JButton btnAdvancedSearch;
	private JTextField textField = new JTextField("Search here");
	private String[] jbtn_name = {"House name", "Description", "Neighborhood", "Property"};
	private int[] searchTabel = new int[4];
	private int num = 15;
	private String pre_sql, submit_sql, delete_sql, index_sql;

	public static void main(String args[]){
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					AirbnbInterface frame = new AirbnbInterface();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	public AirbnbInterface() {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 400, 250);
		cp = this.getContentPane();
		
		basicSearch();
		cp.validate();
	}
	
	public void addMenu() {
		JMenuBar menu=new JMenuBar();  
		this.setJMenuBar(menu);
		        
		JMenuItem search = new JMenuItem("Search");
		search.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				basicSearch();
			}
		});
		menu.add(search);
			        
		JMenuItem pre_query = new JMenuItem("Predefined Queries");  
		pre_query.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				presentQuery();
			}
		});
		menu.add(pre_query);
			        
		JMenuItem ins_del=new JMenuItem("Insert/Delete"); 
		ins_del.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				Insert_Delete();
			}
		});
		menu.add(ins_del);
	}
	
	private void clearPanel() {
		if (mainPanel != null) cp.remove(mainPanel);
		if (advancePanel != null) cp.remove(advancePanel);
		if (operationPanel != null) cp.remove(operationPanel);
		if (infoPanel != null) cp.remove(infoPanel);
		if (selectionPanel != null) cp.remove(selectionPanel);
		if (parameterPanel != null) cp.remove(parameterPanel);
		if (presentPanel != null) cp.remove(presentPanel);
		if (resultPanel != null) cp.remove(resultPanel);
		if (resultScroller != null) cp.remove(resultScroller);
		if (desScroller != null) cp.remove(desScroller);
		if (presentScroller != null) cp.remove(presentScroller);
		return;
	}
	
	private void basicSearch() {
		clearPanel();
		setBounds(100, 100, 400, 250);
		
		mainPanel = new JPanel();
		mainPanel.setPreferredSize(new Dimension(400, 200));
		mainPanel.setLayout(null);
		
		textField.setBounds(60, 40, 280, 25);
		textField.setColumns(10);
		textField.setEditable(true);
		textField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyPressed(KeyEvent e) {
				if (e.getKeyCode()==KeyEvent.VK_ENTER) {
					String str = textField.getText();
					Arrays.fill(searchTabel, 1);
					presentResult(str);	
				}
			}
		});
		mainPanel.add(textField);
		
		btnAdvancedSearch = new JButton("Advanced Option");
		btnAdvancedSearch.setBounds(250, 80, 140, 30);
		mainPanel.add(btnAdvancedSearch);
		AdvanceSearch();
		
		addMenu();
		advancePanel = new JPanel();
		advancePanel.setPreferredSize(new Dimension(400, 50));
		cp.add(mainPanel, BorderLayout.NORTH);
		cp.add(advancePanel, BorderLayout.CENTER);		
		
		pack();
	}
	
	private void AdvanceSearch() {
		btnAdvancedSearch.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (advancePanel!=null) cp.remove(advancePanel);
				advancePanel = new JPanel(new GridLayout(6, 1));
				advancePanel.setPreferredSize(new Dimension(400, 150));
				Arrays.fill(searchTabel, 0);
				
				Label label = new Label("Tabels to search:");
				advancePanel.add(label);
				
				JRadioButton rdbtn[] = new JRadioButton[4];
				for(int i=0; i<jbtn_name.length; i++) {
					rdbtn[i] = new JRadioButton(jbtn_name[i]);
					rdbtn[i].setActionCommand(jbtn_name[i]);
					rdbtn[i].addActionListener(new ActionListener() {
						@Override
						public void actionPerformed(ActionEvent e) {
							switch (e.getActionCommand()) {
								case "House name":
									searchTabel[0]=1;
									break;
								case "Description":
									searchTabel[1]=1;
									break;
								case "Neighborhood":
									searchTabel[2]=1;
									break;
								case "Property":
									searchTabel[3]=1;
									break;
								default:
									break;
							}
						}
					});	
					advancePanel.add(rdbtn[i]);
				}
				
				JButton btn = new JButton("Apply changes");
				btn.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						String str = textField.getText();
						presentResult(str);
					}
				});	
				
				advancePanel.add(btn);
				cp.add(advancePanel, BorderLayout.CENTER);
				pack();
			}
		});
	}
	
	private void presentResult(String str) {
		clearPanel();
		setBounds(100, 100, 400, 350);
		
		resultPanel = new JPanel();
		resultPanel.setBackground(Color.LIGHT_GRAY);
		resultPanel.setLayout(null);
		
		resultScroller = new JScrollPane(resultPanel);
		resultScroller.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		resultScroller.setViewportView(resultPanel);
		
		JTextArea[] tfgrp = new JTextArea[num];
		JLabel[] lblgrp = new JLabel[num];
		JButton[] btngrp = new JButton[num];
		String[] name = new String[num];
		int[] price = new int[num];
		int[] hid = new int[num];
		int n=0;
		String[] sql = {"SELECT id FROM house WHERE instr(name,'" + str +"')>0",
						"SELECT id FROM house WHERE instr(description1,'" + str +"')>0 "
								+ "OR instr(description2,'" + str +"')>0",
						"SELECT id FROM neighborhood WHERE instr(neighborhood,'" + str + "')>0",
						"SELECT id FROM house_property, property WHERE house_property.pid=property.pid "
								+ "AND instr(property.p_description,'" + str + "')>0"};
		
		try {
			Connection conn = null;
			PreparedStatement pre = null;
			ResultSet rs = null;
			
			Class.forName(driverUrl);
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			System.out.println("Connection successed.");
			
			
			for (int i=0; i<4; i++) {
				if (searchTabel[i]==1) {
					pre = conn.prepareStatement(sql[i]);
					rs = pre.executeQuery(sql[i]);
			
					while (rs.next() && n<num) {
						hid[n] = rs.getInt("id");
						n++;
					}
				}
			}
			
			for (int i=0; i<Math.min(n,num); i++) {
				String present_sql = "SELECT reference_daily FROM price " 
									+ "WHERE id=" + hid[i]; 
				pre = conn.prepareStatement(present_sql);
				rs = pre.executeQuery(present_sql);
				rs.next();
				price[i] = rs.getInt("reference_daily");
				
				present_sql = "SELECT name FROM house " 
							+ "WHERE id=" + hid[i];
				pre = conn.prepareStatement(present_sql);
				rs = pre.executeQuery(present_sql);
				rs.next();
				name[i] = rs.getString("name");
			}
			
			conn.close();
			System.out.println("Connection closed.");
		} catch (Exception e) {
			System.err.println("Got an exception!");
			System.err.println(e.getMessage());
		}
	
		resultScroller.setPreferredSize(new Dimension(400, 40 * Math.min(n, num)));
		resultPanel.setPreferredSize(new Dimension(380, 40 * Math.min(n, num)));
		
		int[] axis_X = {20, 220, 300};
		int axis_Y=0;
		for (int i=0; i<Math.min(n,num); i++) {	
			int m=i;
			
			axis_Y = m*40;
			tfgrp[m] = new JTextArea();
			tfgrp[m].setText(name[m]);
			tfgrp[m].setLineWrap(true);
			tfgrp[m].setBackground(Color.LIGHT_GRAY);
			tfgrp[m].setBounds(axis_X[0], axis_Y, 180, 40);
			
			lblgrp[m] = new JLabel();
			lblgrp[m].setText("$" + price[m]);
			lblgrp[m].setBounds(axis_X[1], axis_Y, 60, 20);
			
			btngrp[m] = new JButton("Details");
			btngrp[m].setBounds(axis_X[2], axis_Y, 80, 20);
			
			btngrp[m].addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					showDetail(hid[m]);
				}
			});
			
			resultPanel.add(tfgrp[m]);
			resultPanel.add(lblgrp[m]);
			resultPanel.add(btngrp[m]);
		}
		
		cp.add(resultScroller, BorderLayout.CENTER);
	}
	
	private void presentQuery() {
		clearPanel();
		setBounds(100, 100, 400, 300);
		selectionPanel = new JPanel();
		selectionPanel.setPreferredSize(new Dimension(400,150));
		selectionPanel.setLayout(null);
		selectionPanel.setBackground(Color.GRAY);
		
		String[] ta_name ={"Average price with specified number of \nbedrooms",
							"Find 20 hosts with available property \nbetween two dates",
							"Find the 5 most cheapest available \napartment"};
		JTextArea taQuery[] = new JTextArea[3];
		JButton btnQuery[] = new JButton[3];
 		for(int i=0; i<ta_name.length; i++) {
			taQuery[i] = new JTextArea();
			taQuery[i].setText(ta_name[i]);
			taQuery[i].setBounds(20, 20+40*i, 280, 30);
			taQuery[i].setBackground(Color.GRAY);
			taQuery[i].setForeground(Color.WHITE);
			taQuery[i].setLineWrap(true);
			
			btnQuery[i] = new JButton("Select");
			btnQuery[i].setBounds(310, 20+40*i, 70, 30);
			btnQuery[i].setActionCommand(i + "");
			btnQuery[i].addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					if (parameterPanel!=null) cp.remove(parameterPanel);
					parameterPanel = new JPanel();
					parameterPanel.setPreferredSize(new Dimension(400,150));
					parameterPanel.setLayout(null);
					parameterPanel.setBackground(Color.LIGHT_GRAY);
					
					switch (e.getActionCommand()) {
						case "0":
							Label label1 = new Label("Number of beds:");
							label1.setBounds(20, 20, 120, 30);
							parameterPanel.add(label1);
							
							JTextField tfP1 = new JTextField();
							tfP1.setBounds(150, 20, 200, 30);
							parameterPanel.add(tfP1);
							
							JButton submit = new JButton("Go!");
							submit.setBounds(300, 60, 80, 30);
							submit.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									pre_sql="SELECT AVG(P.reference_daily) " +
											"FROM price P, house H " +
											"WHERE H.no_bedroom=" + tfP1.getText() + " AND P.id=H.id";
									query1();
								}
							});
							parameterPanel.add(submit);
							break;
						case "1":
							label1 = new Label("From");
							label1.setBounds(20, 20, 60, 30);
							parameterPanel.add(label1);
							
							tfP1 = new JTextField("01/03/2019");
							tfP1.setBounds(80, 20, 100, 30);
							parameterPanel.add(tfP1);
							
							Label label2 = new Label("to");
							label2.setBounds(190, 20, 30, 30);
							parameterPanel.add(label2);
							
							JTextField tfP2 = new JTextField("30/03/2019");
							tfP2.setBounds(220, 20, 100, 30);
							parameterPanel.add(tfP2);
							
							submit = new JButton("Go!");
							submit.setBounds(300, 60, 80, 30);
							submit.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									pre_sql="SELECT DISTINCT H.host_id " + 
											"FROM host H, house_refer_calendar I, house_own J " + 
											"WHERE H.host_id=J.host_id AND I.id=J.id " + 
											"AND I.available='t' " + 
											"AND I.datee between to_date('" + tfP1.getText() + "','DD/MM/YYYY') " +
											"AND to_date('" + tfP2.getText() + "','DD/MM/YYYY')";
									query2();
								}
							});
							parameterPanel.add(submit);
							break;
						case "2":
							cp.setBounds(100, 100, 400, 650);
							parameterPanel.setPreferredSize(new Dimension(400,400));
							
							Label label0 = new Label("City:");
							label0.setBounds(20, 20, 60, 30);
							parameterPanel.add(label0);
							
							String city_name[] = {"Barcelone ", "Berlin", "Madrid"};
							JComboBox<String> cityList = new JComboBox<>(city_name);
							cityList.setBounds(80, 20, 140, 30);
							parameterPanel.add(cityList);
							
							label1 = new Label("Number of beds:");
							label1.setBounds(20, 50, 110, 30);
							parameterPanel.add(label1);
							
							tfP1 = new JTextField("1");
							tfP1.setBounds(140, 50, 100, 30);
							parameterPanel.add(tfP1);
							
							Label label3 = new Label("From");
							label3.setBounds(20, 80, 60, 30);
							parameterPanel.add(label3);
							
							tfP2 = new JTextField("01/03/2019");
							tfP2.setBounds(80, 80, 100, 30);
							parameterPanel.add(tfP2);
							
							Label label4 = new Label(" to");
							label4.setBounds(180, 80, 20, 30);
							parameterPanel.add(label4);
							
							JTextField tfP3 = new JTextField("03/03/2019");
							tfP3.setBounds(200, 80, 100, 30);
							parameterPanel.add(tfP3);
							
							Label label5 = new Label("Cancellation policy:");
							label5.setBounds(20, 110, 150, 30);
							parameterPanel.add(label5);
							
							String cancel[] = {"flexible", "moderate", "strict", "strict_14_with_grace_period", "super_strict_30", "super_strict_60"};
							JComboBox<String> cancelList = new JComboBox<>(cancel);
							cancelList.setBounds(170, 110, 200, 30);
							parameterPanel.add(cancelList);
							
							Label label6 = new Label("Minimum location review score:");
							label6.setBounds(20, 140, 200, 30);
							parameterPanel.add(label6);
							
							String score[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};
							
							JComboBox<String> scoreList = new JComboBox<>(score);
							scoreList.setBounds(220, 140, 150, 30);
							parameterPanel.add(scoreList);
							
							Label label7 = new Label("Property:");
							label7.setBounds(20, 170, 150, 30);
							parameterPanel.add(label7);
							
							String property[] = {"Aparthotel", "Apartment", "Bed and breakfast", "Boat", "Boutique hotel", "Bungalow",
											"Cabin", "Camper/RV", "Casa particular(Cuba)", "Castle", "Cave", "Chalet", "Condominium",
											"Cottage", "Dome house", "Dorm", "Earth house", "Farm stay", "Guest suite", "Guesthouse",
											"Hostel", "Hotel", "House", "Houseboat", "Hut", "Loft", "Other", "Pension (South Korea)",
											"Resort", "Serviced apartment", "Tiny house", "Townhouse", "Villa"};
							JComboBox<String> propertyList = new JComboBox<>(property);
							propertyList.setBounds(170, 170, 210, 30);
							parameterPanel.add(propertyList);
						
							Label label8 = new Label("Host verification:");
							label8.setBounds(20, 200, 200, 30);
							parameterPanel.add(label8);
							
							String verify[] = {"email", "facebook", "google", "jumio", 
												"kba", "phone", "sent_id", "reviews",
												"sesame", "selfie", "work_email", "weibo",
												"government-id", "indentity-manual", 
												"photographer", "manual_offline", 
												"manual_online", "offline_government_id",
												"sesame-offline", "zhima_selfie"};
							JCheckBox[] verification = new JCheckBox[20];
					        for (int i=0; i<12; i++) {
					        	verification[i] = new JCheckBox();
					        	verification[i].setText(verify[i]);
					        	verification[i].setEnabled(true);
					        	verification[i].setForeground(Color.BLACK);
					        	verification[i].setBounds(15+i%4*95, 230+i/4*20, 95, 20);
					        	parameterPanel.add(verification[i]);
					        }
					        for (int i=12; i<20; i++) {
					        	verification[i] = new JCheckBox();
					        	verification[i].setText(verify[i]);
					        	verification[i].setEnabled(true);
					        	verification[i].setForeground(Color.BLACK);
					        	verification[i].setBounds(15+(i-12)%2*190, 290+(i-12)/2*20, 180, 20);
					        	parameterPanel.add(verification[i]);
					        }
							
					        
							submit = new JButton("Go!");
							submit.setBounds(300, 370, 100, 30);
							submit.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									pre_sql="SELECT id FROM(SELECT h1.id,avg(hrc.price) as price " + 
											"FROM have_city hc,country co,house h1,house_bed_type hb,score s,house_refer_calendar hrc,verify v," +
											"verification v2,cancel ca,cancellation can,host,house_own ho,property p,house_property hp " + 
											"WHERE hc.city_code=co.city_code AND co.id=h1.id AND hb.id=h1.id AND s.id=h1.id " + 
											"AND hrc.id=h1.id AND v.vid=v2.vid AND v2.host_id=ho.host_id AND ho.id=h1.id " + 
											"AND ca.cid=can.cid AND can.id=h1.id AND p.pid=hp.pid AND hp.id=h1.id AND hrc.available='t' " + 
											"AND hrc.datee between to_date('" + tfP2.getText() + "','DD/MM/YYYY') and to_date('" + tfP3.getText() + "','DD/MM/YYYY') " + 
											"AND hc.city_name='" + cityList.getSelectedItem() +"' AND hb.no_of_bed>=" + tfP1.getText() + " AND s.location>=" + 
											scoreList.getSelectedItem() + " AND ca.c_description='" + cancelList.getSelectedItem() + "' " + 
											"AND p. p_description='" + propertyList.getSelectedItem() + "'";
									
									String str1="";
									int num=0;
									for (int i=0; i<20; i++) {
										if (verification[i].isSelected() == true && num!=0) str1 = str1 + ",'" + verify[i] + "'";
										else if (verification[i].isSelected() == true && num==0) {
											num++;
											str1 = "'" + verify[i] + "'";
										}
									}	
									
									pre_sql = pre_sql + " AND v.vid in (SELECT v.vid FROM verify v WHERE v.v_description=ANY(" + str1 + "))";
									pre_sql = pre_sql + " GROUP BY h1.id,p.p_description ORDER BY avg(hrc.price) desc) WHERE rownum<=5";
									query3();
								}
							});
							parameterPanel.add(submit);
							break;
						default:
							break;
					}
					
					cp.add(parameterPanel, BorderLayout.CENTER);
					pack();
					validate();
				}
			});
			
			selectionPanel.add(taQuery[i]);
			selectionPanel.add(btnQuery[i]);
		}
		
		cp.add(selectionPanel, BorderLayout.NORTH);
		pack();
	}
	
	private void Insert_Delete() {
		clearPanel();
		setBounds(100, 100, 400, 250);
		operationPanel = new JPanel();
		operationPanel.setPreferredSize(new Dimension(120,250));
		operationPanel.setLayout(new GridLayout(5, 1));
		operationPanel.setBackground(Color.GRAY);
		
		JTextArea title = new JTextArea("\n  Choose tabel to\n  insert/delete:");
		title.setLineWrap(true);
		title.setForeground(Color.WHITE);
		title.setBackground(Color.GRAY);
		operationPanel.add(title);
		
		String[] table = {"Location", "Room Type", "Amenities", "House Property"};
		JButton[] tableList = new JButton[4];
		for(int i=0; i<4; i++) {
			tableList[i] = new JButton(table[i]);
			tableList[i].setActionCommand(i + "");
			tableList[i].addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					if (infoPanel!=null) cp.remove(infoPanel);
					infoPanel = new JPanel();
					infoPanel.setPreferredSize(new Dimension(280,250));
					infoPanel.setLayout(null);
					infoPanel.setBackground(Color.LIGHT_GRAY);
					
					switch(e.getActionCommand()) {
						case "0":
							Label country = new Label("<html>Country<br></br>code: ");
							country.setBounds(20, 20, 80, 30);
							infoPanel.add(country);
							JTextField tf_country = new JTextField();
							tf_country.setBounds(100, 20, 180, 30);
							infoPanel.add(tf_country);
							Label city = new Label("City: ");
							city.setBounds(20, 60, 80, 30);
							infoPanel.add(city);
							JTextField tf_city = new JTextField();
							tf_city.setBounds(100, 60, 180, 30);
							infoPanel.add(tf_city);
							
							submit_sql = "INSERT INTO have_city VALUES (?,?,?)";
							index_sql = "SELECT MAX (city_code) FROM have_city";
							
							JButton submit = new JButton("Submit");
							submit.setBounds(200, 100, 60, 20);
							submit.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									try {
										Connection conn = null;
										PreparedStatement pre = null;
										ResultSet rs = null;
										
										Class.forName(driverUrl);
										conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
										System.out.println("Connection successed.");
										
										pre = conn.prepareStatement(index_sql);
										rs = pre.executeQuery(index_sql);
										rs.next();
										int index = rs.getInt("MAX(city_code)") + 1;
			
										pre = conn.prepareStatement(submit_sql);
										pre.setString(1, tf_city.getText());
										pre.setInt(2, index);
										pre.setString(3, tf_country.getText());
										pre.executeUpdate();
										
										JOptionPane.showMessageDialog(null, "One record has been succesfully added to the database");
										conn.close();
										System.out.println("Connection closed.");
									} catch (Exception exc) {
										System.err.println("Got an exception!");
										System.err.println(exc.getMessage());
									}
								}
							});
							infoPanel.add(submit);
							
							JButton delete = new JButton("Delete");
							delete.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									try {
										Connection conn = null;
										PreparedStatement pre = null;
										
										Class.forName(driverUrl);
										conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
										System.out.println("Connection successed.");
										
										delete_sql = "DELETE FROM have_city WHERE city_name='" + tf_city.getText() + 
													"' AND country_code='" + tf_country.getText() + "'";
										pre = conn.prepareStatement(delete_sql);
										pre.executeUpdate();
										
										JOptionPane.showMessageDialog(null, "One record has been succesfully deleted to the database");
										conn.close();
										System.out.println("Connection closed.");
									} catch (Exception exc) {
										System.err.println("Got an exception!");
										System.err.println(exc.getMessage());
									}
								}
							});
							delete.setBounds(200, 130, 60, 20);
							infoPanel.add(delete);
							break;
						case "1":
							Label roomType = new Label("Room Type: ");
							roomType.setBounds(20, 20, 80, 30);
							infoPanel.add(roomType);
							JTextField tf_roomType = new JTextField();
							tf_roomType.setBounds(100, 20, 180, 30);
							infoPanel.add(tf_roomType);
					
							submit_sql = "INSERT INTO room_type values (?,?)";
							index_sql = "SELECT MAX(tid) FROM room_type";
							delete_sql = "DELETE FROM room_type WHERE ?,? ";
							
							submit = new JButton("Submit");
							submit.setBounds(200, 100, 60, 20);
							submit.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									try {
										Connection conn = null;
										PreparedStatement pre = null;
										ResultSet rs = null;
										
										Class.forName(driverUrl);
										conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
										System.out.println("Connection successed.");
										
										pre = conn.prepareStatement(index_sql);
										rs = pre.executeQuery(index_sql);
										rs.next();
										int index = rs.getInt("MAX(tid)") + 1;
			
										pre = conn.prepareStatement(submit_sql);
										pre.setInt(1, index);
										pre.setString(2, tf_roomType.getText());
										pre.executeUpdate();
										
										JOptionPane.showMessageDialog(null, "One record has been succesfully added to the database");
										conn.close();
										System.out.println("Connection closed.");
									} catch (Exception exc) {
										System.err.println("Got an exception!");
										System.err.println(exc.getMessage());
									}
								}
							});
							infoPanel.add(submit);
							
							delete = new JButton("Delete");
							delete.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									try {
										Connection conn = null;
										PreparedStatement pre = null;
										
										Class.forName(driverUrl);
										conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
										System.out.println("Connection successed.");
										
										delete_sql = "DELETE FROM room_type WHERE t_description='" + tf_roomType.getText() + "'";
										pre = conn.prepareStatement(delete_sql);
										pre.executeUpdate();
										
										JOptionPane.showMessageDialog(null, "One record has been succesfully deleted to the database");
										conn.close();
										System.out.println("Connection closed.");
									} catch (Exception exc) {
										System.err.println("Got an exception!");
										System.err.println(exc.getMessage());
									}
								}
							});
							delete.setBounds(200, 130, 60, 20);
							infoPanel.add(delete);
							break;
						case "2":
							Label amenities = new Label("Amenities: ");
							amenities.setBounds(20, 20, 80, 30);
							infoPanel.add(amenities);
							JTextField tf_amenities = new JTextField();
							tf_amenities.setBounds(100, 20, 180, 30);
							infoPanel.add(tf_amenities);
							
							submit_sql = "INSERT INTO amenities_des values (?,?)";
							index_sql = "SELECT MAX(aid) FROM amenities_des";

							submit = new JButton("Submit");
							submit.setBounds(200, 100, 60, 20);
							submit.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									try {
										Connection conn = null;
										PreparedStatement pre = null;
										ResultSet rs = null;
										
										Class.forName(driverUrl);
										conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
										System.out.println("Connection successed.");
										
										pre = conn.prepareStatement(index_sql);
										rs = pre.executeQuery(index_sql);
										rs.next();
										int index = rs.getInt("MAX(aid)") + 1;
			
										pre = conn.prepareStatement(submit_sql);
										pre.setInt(1, index);
										pre.setString(2, tf_amenities.getText());
										pre.executeUpdate();
										
										JOptionPane.showMessageDialog(null, "One record has been succesfully added to the database");
										conn.close();
										System.out.println("Connection closed.");
									} catch (Exception exc) {
										System.err.println("Got an exception!");
										System.err.println(exc.getMessage());
									}
								}
							});
							infoPanel.add(submit);
							
							delete = new JButton("Delete");
							delete.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									try {
										Connection conn = null;
										PreparedStatement pre = null;
										
										Class.forName(driverUrl);
										conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
										System.out.println("Connection successed.");
										
										delete_sql = "DELETE FROM amenities_des WHERE a_des='" + tf_amenities.getText() + "'";
										pre = conn.prepareStatement(delete_sql);
										pre.executeUpdate();
										
										JOptionPane.showMessageDialog(null, "One record has been succesfully deleted to the database");
										conn.close();
										System.out.println("Connection closed.");
									} catch (Exception exc) {
										System.err.println("Got an exception!");
										System.err.println(exc.getMessage());
									}
								}
							});
							delete.setBounds(200, 130, 60, 20);
							infoPanel.add(delete);
							break;
						case "3":
							Label property = new Label("Property: ");
							property.setBounds(20, 20, 80, 30);
							infoPanel.add(property);
							JTextField tf_property = new JTextField();
							tf_property.setBounds(100, 20, 180, 30);
							infoPanel.add(tf_property);
							
							submit_sql = "INSERT INTO property values (?,?)";
							index_sql = "SELECT MAX(pid) FROM property";
							
							submit = new JButton("Submit");
							submit.setBounds(200, 100, 60, 20);
							submit.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									try {
										Connection conn = null;
										PreparedStatement pre = null;
										ResultSet rs = null;
										
										Class.forName(driverUrl);
										conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
										System.out.println("Connection successed.");
										
										pre = conn.prepareStatement(index_sql);
										rs = pre.executeQuery(index_sql);
										rs.next();
										int index = rs.getInt("MAX(pid)") + 1;
			
										pre = conn.prepareStatement(submit_sql);
										pre.setInt(1, index);
										pre.setString(2, tf_property.getText());
										pre.executeUpdate();
										
										JOptionPane.showMessageDialog(null, "One record has been succesfully added to the database");
										conn.close();
										System.out.println("Connection closed.");
									} catch (Exception exc) {
										System.err.println("Got an exception!");
										System.err.println(exc.getMessage());
									}
								}
							});
							infoPanel.add(submit);
							
							delete = new JButton("Delete");
							delete.addActionListener(new ActionListener() {
								@Override
								public void actionPerformed(ActionEvent e) {
									try {
										Connection conn = null;
										PreparedStatement pre = null;
										
										Class.forName(driverUrl);
										conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
										System.out.println("Connection successed.");
										delete_sql = "DELETE FROM property WHERE p_description='" + tf_property.getText() + "'";
										pre = conn.prepareStatement(delete_sql);
										pre.executeUpdate();
										
										JOptionPane.showMessageDialog(null, "One record has been succesfully deleted to the database");
										conn.close();
										System.out.println("Connection closed.");
									} catch (Exception exc) {
										System.err.println("Got an exception!");
										System.err.println(exc.getMessage());
									}
								}
							});
							delete.setBounds(200, 130, 60, 20);
							infoPanel.add(delete);
							break;
						default:
							break;
					}
					
					cp.add(infoPanel, BorderLayout.CENTER);
					pack();
					validate();
				}
			});
			operationPanel.add(tableList[i]);
		}
		
		infoPanel = new JPanel();
		infoPanel.setPreferredSize(new Dimension(280,250));
		infoPanel.setLayout(null);
		infoPanel.setBackground(Color.LIGHT_GRAY);
		
		cp.add(operationPanel, BorderLayout.WEST);
		cp.add(infoPanel, BorderLayout.CENTER);
		pack();
	}
	
	private void showDetail(int listing) {
		clearPanel();
		setBounds(100, 100, 600, 400);
		mainPanel = new JPanel();
		mainPanel.setPreferredSize(new Dimension(600, 250));
		mainPanel.setLayout(null);
		mainPanel.setBackground(Color.WHITE);
		
		String url = "", name = "", des = "";
		int bathroomNum=0, bedroomNum=0, bedNum=0;
		int[] amentities = new int[15];
		String[] a_description = {"Air-con", "intercom", "Elevator", "Essentials", "Family/kid", 
								"Hair dryer", "Hangers", "Heating", "Hot water", "Internet", 
								"Kitchen", "Shampoo", "TV", "Washer", "Wifi"};
		Arrays.fill(amentities, 0);
		try {
			Connection conn = null;
			PreparedStatement pre = null;
			ResultSet rs = null;
			
			Class.forName(driverUrl);
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			System.out.println("Connection successed.");
			
			String sql ="SELECT pic_url, name, no_bathroom, no_bedroom, description1, description2 FROM house WHERE id = " + listing;
			pre = conn.prepareStatement(sql);
			rs = pre.executeQuery(sql);
			rs.next();
			url = rs.getString("pic_url");
			name = rs.getString("name");
			bathroomNum = rs.getInt("no_bathroom");
			bedroomNum = rs.getInt("no_bedroom");
			if (rs.getString("description1") != "null") des = rs.getString("description1");
			if (rs.getString("description2") != "null") des += rs.getString("description2");
			
			
			sql ="SELECT no_of_bed FROM house_bed_type WHERE id = " + listing;
			pre = conn.prepareStatement(sql);
			rs = pre.executeQuery(sql);
			rs.next();
			bedNum = rs.getInt("no_of_bed");
			
			sql ="SELECT aid FROM amenities WHERE id = " + listing;
			pre = conn.prepareStatement(sql);
			rs = pre.executeQuery(sql);
			while(rs.next()) {
				int index = rs.getInt("aid");
				amentities[index-1]=1;
			}
			
			conn.close();
			System.out.println("Connection closed.");
		} catch (Exception e) {
			System.err.println("Got an exception!");
			System.err.println(e.getMessage());
		}
		
		String[] parts = url.split("\\?");
		url = parts[0].trim();
		BufferedImage image = null;
		Image resized_image = null;
			
	    try {
	    	URL pic = new URL(url);
	        image = ImageIO.read(pic);
	        resized_image = image.getScaledInstance(240, 180, Image.SCALE_DEFAULT);
	    } catch (IOException e) {
        	e.printStackTrace();
        }
	        
	    JLabel profile_pic = new JLabel(new ImageIcon(resized_image));
	    profile_pic.setBounds(380, 20, 200, 150);
        mainPanel.add(profile_pic);
        
        JTextArea profile_name = new JTextArea("House name:\n" + name);
        profile_name.setBounds(20, 20, 340, 50);
        profile_name.setEditable(false);
        profile_name.setLineWrap(true);
        profile_name.setBackground(Color.WHITE);
        mainPanel.add(profile_name);
        
        Label title1 = new Label("Facilities:");
        title1.setBounds(20, 80, 200, 20);
        mainPanel.add(title1);
        
        Label profile_bedroom = new Label();
        profile_bedroom.setText(bedroomNum + " bedroom(s)");
        profile_bedroom.setBounds(20, 100, 110, 20);
        mainPanel.add(profile_bedroom);
        
        Label profile_bathroom = new Label();
        profile_bathroom.setText(bathroomNum + " bathroom(s)");
        profile_bathroom.setBounds(140, 100, 110, 20);
        mainPanel.add(profile_bathroom);
        
        Label profile_bed = new Label();
        profile_bed.setText(bedNum + " bed(s)");
        profile_bed.setBounds(260, 100, 110, 20);
        mainPanel.add(profile_bed);
        
        Label title2 = new Label("From");
        title2.setBounds(20, 130, 40, 20);
        mainPanel.add(title2);
        
        JTextField date_start = new JTextField("DD/MM/YYYY");
        date_start.setEditable(true);
        date_start.setBounds(60, 130, 100, 20);
        mainPanel.add(date_start);
        
        Label title3 = new Label("to");
        title3.setBounds(160, 130, 20, 20);
        mainPanel.add(title3);
        
        JTextField date_end = new JTextField("DD/MM/YYYY");
        date_end.setEditable(true);
        date_end.setBounds(180, 130, 100, 20);
        mainPanel.add(date_end);
        
        JButton searchAvail = new JButton("Search");
        searchAvail.setBounds(290, 130, 60, 20);
        searchAvail.addActionListener(new ActionListener() {
        	@Override
        	public void actionPerformed(ActionEvent e) {
        		dateAvailable(date_start.getText(), date_end.getText(), listing);
        	}
        });
        mainPanel.add(searchAvail);
        
        Label title4 = new Label("Amenities:");
        title4.setBounds(20, 160, 80, 20);
        mainPanel.add(title4);
        
        JCheckBox[] cbgrp = new JCheckBox[15];
        for (int i=0; i<15; i++) {
        	cbgrp[i] = new JCheckBox(a_description[i]);
        	cbgrp[i].setEnabled(false);
        	cbgrp[i].setForeground(Color.BLACK);
        	if (amentities[i]==1) cbgrp[i].setSelected(true);
        	else cbgrp[i].setSelected(false);
        	cbgrp[i].setBounds(20+i%5*112, 180+i/5*20, 110, 20);
        	mainPanel.add(cbgrp[i]);
        }
		
        JTextArea profile_des = new JTextArea("Description:\n" + des);
        profile_des.setBounds(20, 180, 560, 200);
        profile_des.setEditable(false);
        profile_des.setLineWrap(true);
        profile_des.setBackground(Color.WHITE);
        desScroller = new JScrollPane();
        desScroller.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
        desScroller.setViewportView(profile_des);
        desScroller.setPreferredSize(new Dimension(600, 200));
        cp.add(desScroller, BorderLayout.SOUTH);
        
		cp.add(mainPanel, BorderLayout.CENTER);
		pack();
		setVisible(true);
	}
	
	private void dateAvailable(String str1, String str2, int listing) {
		SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");
		Date date_start = new Date();
		Date date_end = new Date();
		try {
			date_start = format.parse(str1);
			date_end = format.parse(str2);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Date date = date_start;
		Calendar calendar = new GregorianCalendar(); 
		calendar.setTime(date); 
		Boolean flag = false;
		try {
			Connection conn = null;
			PreparedStatement pre = null;
			ResultSet rs = null;
				
			Class.forName(driverUrl);
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			System.out.println("Connection successed.");
				
			while (date.before(date_end) || date.equals(date_end)) {
				String str = format.format(date);
				String sql = "SELECT id FROM house_refer_calendar WHERE id=" + listing 
							+" AND available='t' AND TO_CHAR(datee, 'DD/MM/YYYY')='" + str + "'";
				pre = conn.prepareStatement(sql);
				rs = pre.executeQuery(sql);
				if (rs.next()) flag = true; 
				else {
					flag = false;
					break;
				}
				calendar.add(Calendar.DATE,1); 
				date=calendar.getTime(); 
			} 
			
			if (flag) {
				date = date_start;
				calendar.setTime(date);
				int tot=0;
				while (date.before(date_end) || date.equals(date_end)) {
					String str = format.format(date);
					String sql = "SELECT price FROM house_refer_calendar WHERE id=" + listing 
								+" AND available='t' AND TO_CHAR(datee, 'DD/MM/YYYY')='" + str + "'";
					pre = conn.prepareStatement(sql);
					rs = pre.executeQuery(sql);
					if (rs.next()) {
						tot = tot + rs.getInt("price");
					} else {
						sql = "SELECT reference_daily FROM price WHERE id=" + listing;
						pre = conn.prepareStatement(sql);
						rs = pre.executeQuery(sql);
						rs.next();
						tot = tot + rs.getInt("reference_daily");
					}
					
					calendar.add(Calendar.DATE,1); 
					date=calendar.getTime(); 
				} 
				JOptionPane.showMessageDialog(null, "The room is available\n"
						+ "Totol price is $" + tot);
			}
			else JOptionPane.showMessageDialog(null, "The room is not available\n");
			
			conn.close();
			System.out.println("Connection closed.");
		} catch (Exception e) {
				System.err.println("Got an exception!");
				System.err.println(e.getMessage());
		}
		return;
	}

	private void query1() {
		try {
			Connection conn = null;
			PreparedStatement pre = null;
			ResultSet rs = null;
			
			Class.forName(driverUrl);
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			System.out.println("Connection successed.");
			
			pre = conn.prepareStatement(pre_sql);
			rs = pre.executeQuery(pre_sql);
			rs.next();
			int avg_price = rs.getInt("AVG(P.reference_daily)");
			
			JOptionPane.showMessageDialog(null, "The room average price is $" + avg_price);
			
			conn.close();
			System.out.println("Connection closed.");
		} catch (Exception e) {
			System.err.println("Got an exception!");
			System.err.println(e.getMessage());
		}
	}
	
	private void query2() {
		clearPanel();
		setBounds(100, 100, 400, 350);
		presentPanel = new JPanel();
		presentPanel.setBackground(Color.LIGHT_GRAY);
		presentPanel.setLayout(null);
		
		presentScroller = new JScrollPane(presentPanel);
		presentScroller.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		presentScroller.setViewportView(presentPanel);
		
		JTextArea[] tfgrp = new JTextArea[20];
		JLabel[] host_pic = new JLabel[20];
		JButton[] btngrp = new JButton[20];
		String[] name = new String[20];
		String[] pic = new String[20];
		int[] hid = new int[20];
		int[] listing = new int[20];
		int index=0;
		
		try {
			Connection conn = null;
			PreparedStatement pre = null;
			ResultSet rs = null;
			
			Class.forName(driverUrl);
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			System.out.println("Connection successed.");
			
			pre = conn.prepareStatement(pre_sql);
			rs = pre.executeQuery(pre_sql);
			while (rs.next() && index<20) {
				hid[index] = rs.getInt("host_id");
				index++;
			}
			
			for (int i=0; i<index; i++) {
				String present_sql = "SELECT name FROM host " 
									+ "WHERE host_id=" + hid[i]; 
				pre = conn.prepareStatement(present_sql);
				rs = pre.executeQuery(present_sql);
				rs.next();
				name[i] = rs.getString("name");

				present_sql = "SELECT pic_url FROM contains_info " 
							+ "WHERE host_id=" + hid[i];
				pre = conn.prepareStatement(present_sql);
				rs = pre.executeQuery(present_sql);
				rs.next();
				pic[i] = rs.getString("pic_url");
				
				present_sql = "SELECT id FROM house_own " 
							+ "WHERE host_id=" + hid[i];
				pre = conn.prepareStatement(present_sql);
				rs = pre.executeQuery(present_sql);
				rs.next();
				listing[i] = rs.getInt("id");
			}
			
			conn.close();
			System.out.println("Connection closed.");
		} catch (Exception e) {
			System.err.println("Got an exception!");
			System.err.println(e.getMessage());
		}
		
		presentScroller.setPreferredSize(new Dimension(400, 350));
		presentPanel.setPreferredSize(new Dimension(380, index*100+80));
		int[] axis_X = {20, 150, 300};
		int axis_Y=0;
		for (int i=0; i<index; i++) {	
			int m=i;
			
			axis_Y = 20+m*100;			
			tfgrp[m] = new JTextArea();
			tfgrp[m].setLineWrap(true);
			tfgrp[m].setBackground(Color.LIGHT_GRAY);
			tfgrp[m].setText("Host name:\n" + name[m]);
			tfgrp[m].setBounds(axis_X[0], axis_Y, 100, 50);
			
			String[] parts = pic[i].split("\\?");
			pic[i] = parts[0].trim();
			BufferedImage image = null;
			Image resized_image = null;
				
		    try {
		    	URL pic_url = new URL(pic[i]);
		        image = ImageIO.read(pic_url);
		        resized_image = image.getScaledInstance(127, 95, Image.SCALE_DEFAULT);
		    } catch (IOException e) {
	        	e.printStackTrace();
	        }
		        
		    host_pic[m] = new JLabel(new ImageIcon(resized_image));
		    host_pic[m].setBounds(axis_X[1], axis_Y, 127, 95);
			
			btngrp[m] = new JButton("Details");
			btngrp[m].setBounds(axis_X[2], axis_Y, 80, 20);
			
			btngrp[m].addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					showDetail(listing[m]);
				}
			});
			
			presentPanel.add(host_pic[m]);
			presentPanel.add(tfgrp[m]);
			presentPanel.add(btngrp[m]);
		}
		
		JButton back = new JButton("Back");
		back.setBounds(300, axis_Y+110, 80, 30);
		back.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				presentQuery();
			}
		});
		presentPanel.add(back);
		
		cp.add(presentScroller, BorderLayout.CENTER);
		validate();
	}
	
	private void query3() {
		clearPanel();
		setBounds(100, 100, 400, 350);
		presentPanel = new JPanel();
		presentPanel.setBackground(Color.LIGHT_GRAY);
		presentPanel.setLayout(null);
		
		JTextArea[] tfgrp = new JTextArea[5];
		JLabel[] lblgrp = new JLabel[5];
		JButton[] btngrp = new JButton[5];
		String[] name = new String[5];
		int[] price = new int[5];
		int[] hid = new int[5];
		int index=0;
		
		try {
			Connection conn = null;
			PreparedStatement pre = null;
			ResultSet rs = null;
			
			Class.forName(driverUrl);
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			System.out.println("Connection successed.");
			
			pre = conn.prepareStatement(pre_sql);
			rs = pre.executeQuery(pre_sql);
			while (rs.next()) {
				hid[index] = rs.getInt("id");
				index++;
			}
			
			for (int i=0; i<index; i++) {
				String present_sql = "SELECT reference_daily FROM price " 
									+ "WHERE id=" + hid[i]; 
				pre = conn.prepareStatement(present_sql);
				rs = pre.executeQuery(present_sql);
				rs.next();
				price[i] = rs.getInt("reference_daily");

				present_sql = "SELECT name FROM house " 
							+ "WHERE id=" + hid[i];
				pre = conn.prepareStatement(present_sql);
				rs = pre.executeQuery(present_sql);
				rs.next();
				name[i] = rs.getString("name");
			}
			
			conn.close();
			System.out.println("Connection closed.");
		} catch (Exception e) {
			System.err.println("Got an exception!");
			System.err.println(e.getMessage());
		}
		
		int[] axis_X = {20, 220, 300};
		int axis_Y=0;
		for (int i=0; i<Math.min(5,index); i++) {	
			int m=i;
			
			axis_Y = 20+m*40;
			tfgrp[m] = new JTextArea();
			tfgrp[m].setText(name[m]);
			tfgrp[m].setLineWrap(true);
			tfgrp[m].setBackground(Color.LIGHT_GRAY);
			tfgrp[m].setBounds(axis_X[0], axis_Y, 180, 40);
			
			lblgrp[m] = new JLabel();
			lblgrp[m].setText("$" + price[m]);
			lblgrp[m].setBounds(axis_X[1], axis_Y, 60, 20);
			
			btngrp[m] = new JButton("Details");
			btngrp[m].setBounds(axis_X[2], axis_Y, 80, 20);
			
			btngrp[m].addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					showDetail(hid[m]);
				}
			});
			
			presentPanel.add(tfgrp[m]);
			presentPanel.add(lblgrp[m]);
			presentPanel.add(btngrp[m]);
		}
		
		JButton back = new JButton("Back");
		back.setBounds(300, axis_Y+40, 80, 30);
		back.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				presentQuery();
			}
		});
		presentPanel.add(back);
		
		cp.add(presentPanel, BorderLayout.CENTER);
		validate();
	}
}
