package h2.start;

import java.awt.Color;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ScrollPaneConstants;
import javax.swing.border.LineBorder;

public class StartGui extends JFrame {
	private static final long serialVersionUID = 1L;
	int v = ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS;
	int h = ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER;
	private JTabbedPane tabs = new JTabbedPane();
	//tab panels
	private JPanel panel1 = new JPanel();
	private JPanel panel2 = new JPanel();
	private JPanel panel3 = new JPanel();
	//tab layout
	private JPanel upPanel1 = new JPanel();
	private JPanel downPanel1 = new JPanel();
	private JPanel upPanel2 = new JPanel();
	private JPanel downPanel2 = new JPanel();
	private JPanel tablePanel2= new JPanel();
	private JPanel upPanel3 = new JPanel();
	private JPanel downPanel3 = new JPanel();
	private JPanel tablePanel3= new JPanel();
	//tab 1 label
	private JLabel idCustumerLabel =new JLabel("ID custumer");
	private JLabel firstNameLabel = new JLabel("First Name");
	private JLabel lastNameLabel = new JLabel("Last Name");
	private JLabel emailLabel = new JLabel("E-mail(UNIQUE FIELD)");
	private JLabel phoneLabel = new JLabel("Phone");
	private JLabel addressLabel = new JLabel("Address");
	//tab 2 label
	private JLabel idCarLebel = new JLabel("ID car");
	private JLabel makeLabel = new JLabel("Make");
	private JLabel modelLabel = new JLabel("Model");
	private JLabel numberLabel = new JLabel("No(!UNIQUE FIELD)");
	private JLabel priceLabel = new JLabel("Price");
	private JLabel yearLabel = new JLabel("Year");
	//tab 3 label
	private JLabel idLabel3= new JLabel("ID Orders");
	private JLabel firstName3Label = new JLabel("firstName");
	private JLabel lastName3Label = new JLabel("lastName");
	private JLabel email3Label = new JLabel("e-mail");
	private JLabel phone3Label = new JLabel("Phone");
	private JLabel addres3Label = new JLabel("Address");
	private JLabel make3Label = new JLabel("Make");
	private JLabel model3Label = new JLabel("Model");
	private JLabel numbe3Label = new JLabel("Car number");
	private JLabel prise3Label = new JLabel("Price");
	private JLabel year3Label = new JLabel("year");
	//tab 1 text field
	private JTextField idCustumer = new JTextField();
	private JTextField firstName = new JTextField();
	private JTextField lastName = new JTextField();
	private JTextField eMail = new JTextField();
	private JTextField phone = new JTextField();
	private JTextField addressName = new JTextField();
	//tab 2 text field
	private JTextField idCar = new JTextField();
	private JTextField make = new JTextField();
	private JTextField model = new JTextField();
	private JTextField number = new JTextField();
	private JTextField price = new JTextField();
	private JTextField year = new JTextField();
	//tab 3 text field
	private JTextField idCustumer3 = new JTextField();
	private JTextField firstName3 = new JTextField();
	private JTextField lastName3 = new JTextField();
	private JTextField email3 = new JTextField();
	private JTextField phone3 = new JTextField();
	private JTextField addres3 = new JTextField();
	private JTextField make3 = new JTextField();
	private JTextField model3 = new JTextField();
	private JTextField number3 = new JTextField();
	private JTextField price3 = new JTextField();
	private JTextField year3 = new JTextField();
	//buttons for tabs 1, 2, 3
	private JButton viewCustumers = new JButton("VIEW CUSTUMERS");
	private JButton addButton = new JButton("ADD");
	private JButton upDateButton = new JButton("UPDATE");
	private JButton deleteButton = new JButton("DELETE");
	private JButton viewCars = new JButton("VIEW CARS");
	private JButton addButton2 = new JButton("ADD");
	private JButton upDateButton2 = new JButton("UPDATE");
	private JButton deleteButton2 = new JButton("DELETE");
	private JButton viewOrders = new JButton("VIEW ORDERS");
	private JButton addButton3 = new JButton("ADD");
	private JButton deleteButton3 = new JButton("DELETE");

	Connection conn; //connection variable
	//table for tab 1, 2, 3
	JTable myTable = new JTable();
	JTable myTable2 = new JTable();
	JTable myTable3 = new JTable();
	//scroll for table
	JScrollPane scroler = new JScrollPane(myTable, v, h);
	JScrollPane scroler2 = new JScrollPane(myTable2, v, h);
	JScrollPane scroler3 = new JScrollPane(myTable3, v, h);
	
	//constructor for program frame
	public StartGui() {
	//Create and set up main frame
	JFrame frame = new JFrame("Car Shop");
	frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	frame.setSize(1000, 800);
	frame.setLayout(new GridLayout(1,1));
	frame.add(tabs);
	frame.setVisible(true);
	//add tabs
	tabs.addTab("Custumers", panel1);
	tabs.addTab("Cars", panel2);
	tabs.addTab("Orders", panel3);
	//tab1 set
	panel1.setLayout(new GridLayout(3, 1));
	panel1.add(upPanel1);
	panel1.add(downPanel1);
	panel1.add(scroler);
	//tab1 add upPanel content
	upPanel1.setLayout(new GridLayout(6, 2));
	upPanel1.add(idCustumerLabel);
	upPanel1.add(idCustumer);
	upPanel1.add(firstNameLabel);
	upPanel1.add(firstName);
	upPanel1.add(lastNameLabel);
	upPanel1.add(lastName);
	upPanel1.add(emailLabel);
	upPanel1.add(eMail);
	upPanel1.add(phoneLabel);
	upPanel1.add(phone);
	upPanel1.add(addressLabel);
	upPanel1.add(addressName);
	//tab1 add downPanel content
	downPanel1.add(viewCustumers);
	viewCustumers.addActionListener(new ViewAllCustumers());
	downPanel1.add(addButton);
	addButton.addActionListener(new AddCustumer());
	downPanel1.add(upDateButton);
	upDateButton.addActionListener(new UpdateCustumer());
	downPanel1.add(deleteButton);
	deleteButton.addActionListener(new DeleteCustumer());
	//table panel
	scroler.setSize(900, 200);
	scroler.setBorder(new LineBorder(Color.WHITE));
	myTable.getModel();
	myTable.addMouseListener(new MouseAdapter() {
		public void mouseClicked(MouseEvent evt) {
		myTable.getColumnCount();
		int row = myTable.getSelectedRow();
		Object id1 = myTable.getModel().getValueAt(row, 0);
		id1 = id1.toString();
		idCustumer.setText((String) id1);
		firstName.setText((String) myTable.getModel().getValueAt(row, 1));
		lastName.setText((String) myTable.getModel().getValueAt(row, 2));
		eMail.setText((String) myTable.getModel().getValueAt(row, 3));
		Object pn = myTable.getModel().getValueAt(row, 4);
		pn = pn.toString();
		phone.setText((String) pn);
		Object ad = myTable.getModel().getValueAt(row, 5);
		ad = ad.toString();
		addressName.setText((String)ad);
		}
	});
	//tab2 create
	panel2.setLayout(new GridLayout(3, 1));
	panel2.add(upPanel2);
	panel2.add(downPanel2);
	panel2.add(scroler2);
	//tab2 add content
	upPanel2.setLayout(new GridLayout(7, 2));
	upPanel2.add(idCarLebel);
	upPanel2.add(idCar);
	upPanel2.add(makeLabel);
	upPanel2.add(make);
	upPanel2.add(modelLabel);
	upPanel2.add(model);
	upPanel2.add(numberLabel);
	upPanel2.add(number);
	upPanel2.add(priceLabel);
	upPanel2.add(price);
	upPanel2.add(yearLabel);
	upPanel2.add(year);
	//down panel tab2
	downPanel2.add(viewCars);
	viewCars.addActionListener(new ViewAllCars());
	downPanel2.add(addButton2);
	addButton2.addActionListener(new AddCars());
	myTable2.repaint();
	downPanel2.add(upDateButton2);
	upDateButton2.addActionListener(new UpdateCar());
	downPanel2.add(deleteButton2);
	deleteButton2.addActionListener(new DeleteCars());
	downPanel2.add(tablePanel2);
	//table2 panel2
	scroler2.setSize(900, 200);
	scroler2.setBorder(new LineBorder(Color.WHITE));
	myTable2.addMouseListener(new MouseAdapter() {
		public void mouseClicked(MouseEvent evt) {
		myTable2.getColumnCount();
		int row = myTable2.getSelectedRow();
		Object id2 = myTable2.getModel().getValueAt(row, 0);
		id2 = id2.toString();
		idCar.setText((String) id2);
		make.setText((String) myTable2.getModel().getValueAt(row, 1));
		model.setText((String) myTable2.getModel().getValueAt(row, 2));
		Object pri = myTable2.getModel().getValueAt(row, 3);
		pri = pri.toString();
		number.setText((String)pri);
		Object pr = myTable2.getModel().getValueAt(row, 4);
		pr = pr.toString();
		price.setText((String) pr);
		Object y = myTable2.getModel().getValueAt(row, 5);
		y = y.toString();
		year.setText((String) y);
		}
	});
	//tab3 create
	panel3.setLayout(new GridLayout(3, 1));
	panel3.add(upPanel3);
	panel3.add(downPanel3);
	panel3.add(scroler3);
	
	upPanel3.setLayout(new GridLayout(11, 2));
	upPanel3.add(idLabel3);
	upPanel3.add(idCustumer3);
	upPanel3.add(firstName3Label);
	upPanel3.add(firstName3);
	upPanel3.add(lastName3Label);
	upPanel3.add(lastName3);
	upPanel3.add(email3Label);
	upPanel3.add(email3);
	upPanel3.add(phone3Label);
	upPanel3.add(phone3);
	upPanel3.add(addres3Label);
	upPanel3.add(addres3);
	upPanel3.add(addButton3);
	upPanel3.add(make3Label);
	upPanel3.add(make3);
	upPanel3.add(model3Label);
	upPanel3.add(model3);
	upPanel3.add(numbe3Label);
	upPanel3.add(number3);
	upPanel3.add(prise3Label);
	upPanel3.add(price3);
	upPanel3.add(year3Label);
	upPanel3.add(year3);
	
	downPanel3.add(viewOrders);
	viewOrders.addActionListener(new ViewAllOrders());
	downPanel3.add(addButton3);
	addButton3.addActionListener(new AddOrders());
	downPanel3.add(deleteButton3);
	deleteButton3.addActionListener(new DeleteOrder());
	
	//table3 for tab3
	downPanel3.add(tablePanel3);
	scroler3.setSize(900, 200);
	scroler3.setBorder(new LineBorder(Color.WHITE));
	myTable3.addMouseListener(new MouseAdapter() {
		
		public void mouseClicked(MouseEvent evt) {
		myTable3.getColumnCount();
		int row = myTable3.getSelectedRow();
		Object id = myTable3.getModel().getValueAt(row, 0);
		id = id.toString();
		idCustumer3.setText((String)id);
		firstName3.setText((String)myTable3.getModel().getValueAt(row, 1));
		lastName3.setText((String) myTable3.getModel().getValueAt(row, 2));
		email3.setText((String)myTable3.getModel().getValueAt(row, 3));
		Object phon = myTable3.getModel().getValueAt(row, 4);
		phon = phon.toString();
		phone3.setText((String)phon);
		addres3.setText((String)myTable3.getModel().getValueAt(row, 5));
		make3.setText((String)myTable3.getModel().getValueAt(row, 6));
		model3.setText((String)myTable3.getModel().getValueAt(row, 7));
		number3.setText((String)myTable3.getModel().getValueAt(row, 8));
		Object pri = myTable3.getModel().getValueAt(row, 9);
		pri = pri.toString();
		price3.setText((String)pri);
		Object y = myTable3.getModel().getValueAt(row, 10);
		y = y.toString();
		year3.setText((String)y);
		}
	});
}//end constructor
	
	public void refreshContent(){
		try{
			MyTableModel model = selectAll();
			model.fireTableDataChanged();
			myTable.setModel(model);
		} catch(Exception ex){
			ex.printStackTrace();
		}
	}// end refreshContent()
	public MyTableModel selectAll(){
		String sql = "select * from CUSTUMERS";
		conn = DataBaseConection.getDataBaseConection();
		try {
			PreparedStatement prep = conn.prepareStatement(sql);
			ResultSet result = prep.executeQuery();
			MyTableModel model = new MyTableModel(result);
			return model;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}// end selectAll()
	
	//CAR classes
	public void refreshContent2(){
		try{
			MyTableModel model = selectAllCar2();
			model.fireTableDataChanged();
			myTable2.setModel(model);
			myTable2.repaint();
		} catch(Exception ex){}
	}// end refreshContent()
	
	public MyTableModel selectAllCar2(){
		String sql = "select * from CARS";
		conn = DataBaseConection.getDataBaseConection();
		try {
			PreparedStatement prep = conn.prepareStatement(sql);
			ResultSet result = prep.executeQuery();
			MyTableModel model = new MyTableModel(result);
			return model;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}// end selectAllCAR()
	
	class ViewAllCustumers implements ActionListener {
		@Override
		public void actionPerformed(ActionEvent e) {
		String sql = "select * from CUSTUMERS";
		conn = DataBaseConection.getDataBaseConection();
		try {
			PreparedStatement prep = conn.prepareStatement(sql);
			ResultSet result = prep.executeQuery();
			MyTableModel model = new MyTableModel(result);
			model.fireTableDataChanged();
			myTable.setModel(model);
			myTable.repaint();
			prep.close();
			conn.close();
		} catch (SQLException e1) {
			e1.printStackTrace();
			
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
}//end class
	
	class AddCustumer implements ActionListener{

		@Override
		public void actionPerformed(ActionEvent e) {
			int idCu = Integer.parseInt(idCustumer.getText());
			String fName = firstName.getText();
			String lName = lastName.getText();
			String email = eMail.getText();
			int ph = Integer.parseInt(phone.getText());
			String address = addressName.getText();
			if ((fName == null) || (fName == "")) return;
			if ((lName == null) || (lName == "")) return;
			if ((email == null) || (email == "" )) return;
			conn = DataBaseConection.getDataBaseConection();
			String sql = "insert into CUSTUMERS values(?,?,?,?,?,?)";
			try {
				PreparedStatement prep = conn.prepareStatement(sql);
				prep.setInt(1, idCu);
				prep.setString(2, fName);
				prep.setString(3, lName);
				prep.setString(4, email);
				prep.setInt(5, ph);
				prep.setString(6, address);
				prep.execute();
				refreshContent();
				prep.close();
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}// end actionPerformed
}// end Add CUSTUMER
	
	class UpdateCustumer implements ActionListener {

		@Override
		public void actionPerformed(ActionEvent e) {
			int idCu = Integer.parseInt(idCustumer.getText());
			String fName = firstName.getText();
			String lName = lastName.getText();
			String email = eMail.getText();
			int ph = Integer.parseInt(phone.getText());
			String address = addressName.getText();
			conn = DataBaseConection.getDataBaseConection();
			String sql = "update CUSTUMERS set FIRST_NAME = ?, LAST_NAME = ?, EMAIL = ?, PHONE = ?, ADDRESS = ? where ID_CUSTUMER = ?";
			try {
				PreparedStatement prep = conn.prepareStatement(sql);
				prep.setString(1, fName);
				prep.setString(2, lName);
				prep.setString(3, email);
				prep.setInt(4, ph);;
				prep.setString(5, address);
				prep.setInt(6, idCu);
				prep.execute();
				refreshContent();
				prep.close();
				conn.close();
			} catch (SQLException e2) {
				e2.printStackTrace();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}//end class update
	
	class DeleteCustumer implements ActionListener {

		@Override
		public void actionPerformed(ActionEvent e) {
			int idCu = Integer.parseInt(idCustumer.getText());
			conn = DataBaseConection.getDataBaseConection();
			String sql = "delete from CUSTUMERS where ID_CUSTUMER = ?";
			try {
				PreparedStatement prep = conn.prepareStatement(sql);
				prep.setInt(1, idCu);
				prep.execute();
				refreshContent();
				prep.close();
				conn.close();
			} catch (SQLException e2) {
				e2.printStackTrace();
			}
		}
	
	}//end class delete
	class ViewAllCars implements ActionListener {
		@Override
		public void actionPerformed(ActionEvent e) {
		String sql = "select * from CARS";
		conn = DataBaseConection.getDataBaseConection();
		try {
			PreparedStatement prep = conn.prepareStatement(sql);
			ResultSet result = prep.executeQuery();
			MyTableModel model = new MyTableModel(result);
			model.fireTableDataChanged();
			myTable2.setModel(model);
			myTable2.repaint();
			prep.close();
			conn.close();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
	
	class AddCars implements ActionListener{

		@Override
		public void actionPerformed(ActionEvent e) {
			int idC = Integer.parseInt(idCar.getText());
			String mod = model.getText();
			String mak = make.getText();
			String no = number.getText();
			double pr = Double.parseDouble(price.getText());
			int yr = Integer.parseInt(year.getText());
			if ((mod == null) || (mod == "")) return;
			if ((mak == null) || (mak == "")) return;
			if ((no == null) || (no == "" )) return;
			if ((pr <= 0.0)) return;
			if ((yr <= 1920) || (yr >= 2015)) return;
			conn = DataBaseConection.getDataBaseConection();
			String sql = "insert into CARS values(?,?,?,?,?,?)";
			try {
				PreparedStatement prep = conn.prepareStatement(sql);
				prep.setInt(1, idC);
				prep.setString(2, mod);
				prep.setString(3, mak);
				prep.setString(4, no);
				prep.setDouble(5, pr);
				prep.setInt(6, yr);
				prep.execute();
				refreshContent2();
				prep.close();
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}// end add car
	}
	
	class UpdateCar implements ActionListener {

		@Override
		public void actionPerformed(ActionEvent e) {
			int n = Integer.parseInt(idCar.getText());
			String mk = make.getText();
			String mo = model.getText();
			String no = number3.getText();
			double pr = Double.parseDouble(price.getText());
			int yr = Integer.parseInt(year.getText());
			conn = DataBaseConection.getDataBaseConection();
			String sql = "update CARS set MAKE = ?, MODEL = ?, NO = ?, PRICE = ?, YEAR = ? where ID_CAR = ?";
			try {
				PreparedStatement prep = conn.prepareStatement(sql);
				prep.setString(1, mk);
				prep.setString(2, mo);
				prep.setString(3, no);
				prep.setDouble(4, pr);
				prep.setInt(5, yr);
				prep.setInt(6, n);
				prep.execute();
				refreshContent2();
				prep.close();
				conn.close();
			} catch (SQLException e2) {
				e2.printStackTrace();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	class DeleteCars implements ActionListener {

		@Override
		public void actionPerformed(ActionEvent e) {
			int idC = Integer.parseInt(idCar.getText());
			conn = DataBaseConection.getDataBaseConection();
			String sql = "delete from CARS where ID_CAR = ?";
			try {
				PreparedStatement prep = conn.prepareStatement(sql);
				prep.setInt(1, idC);
				prep.execute();
				refreshContent2();
				prep.close();
				conn.close();
			} catch (SQLException e2) {
				e2.printStackTrace();
			}
		}
	}//end class delete car

		public void refreshContent3(){
			try{
				MyTableModel model = selectAllOrders();
				model.fireTableDataChanged();
				myTable3.setModel(model);
				myTable3.repaint();
			}
			catch(Exception ex){}
			}// end refreshContent()

		public MyTableModel selectAllOrders(){
			String sql = "select O.ID_ORDER ,C.FIRST_NAME ,"
					+ "C.LAST_NAME ,C.EMAIL ,C.PHONE ,C.ADDRESS ,"
					+ "K.MAKE ,K.MODEL ,K.NO ,K.PRICE ,"
					+ "K.YEAR FROM ORDERS AS O INNER JOIN CUSTUMERS AS C ON O.ID_CUSTUMER = "
					+ "C.ID_CUSTUMER INNER JOIN CARS AS K ON O.ID_CAR =  "
					+ "K.ID_CAR;";
			conn = DataBaseConection.getDataBaseConection();
			try {
				PreparedStatement prep = conn.prepareStatement(sql);
				ResultSet result = prep.executeQuery();
				MyTableModel model = new MyTableModel(result);
				return model;
			} catch (SQLException e) {
				e.printStackTrace();
				return null;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}// end selectAllOrders

		class ViewAllOrders implements ActionListener {
			@Override
			public void actionPerformed(ActionEvent e) {
			String sql = "select O.ID_ORDER ,C.FIRST_NAME ,"
					+ "C.LAST_NAME ,C.EMAIL ,C.PHONE ,C.ADDRESS ,"
					+ "K.MAKE ,K.MODEL ,K.NO ,K.PRICE ,"
					+ "K.YEAR FROM ORDERS AS O INNER JOIN CUSTUMERS AS C ON O.ID_CUSTUMER = "
					+ "C.ID_CUSTUMER INNER JOIN CARS AS K ON O.ID_CAR =  "
					+ "K.ID_CAR;";
			conn = DataBaseConection.getDataBaseConection();
			try {
				PreparedStatement prep = conn.prepareStatement(sql);
				ResultSet result = prep.executeQuery();
				MyTableModel model = new MyTableModel(result);
				model.fireTableDataChanged();
				myTable3.setModel(model);
				myTable3.repaint();
				prep.close();
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
				
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	} //end ViewAllOrders class

		class AddOrders implements ActionListener{
		
			@Override
				public void actionPerformed(ActionEvent e) {
					conn = DataBaseConection.getDataBaseConection();
					System.out.println("add");
					int e1 = Integer.parseInt(JOptionPane.showInputDialog(tabs,"Enter the id for client:"));
					int no = Integer.parseInt(JOptionPane.showInputDialog(tabs,"Enter the id for car:"));
					String sql = "insert into ORDERS values(null, ?, ?)";
					try {
						PreparedStatement prep = conn.prepareStatement(sql);
							prep.setInt(1, e1);
							prep.setInt(2, no);
							prep.execute();
							refreshContent3();
						prep.close();
						conn.close();
					} catch (SQLException e11) {
						e11.printStackTrace();
					}
				}
			}// end AddAction

		class DeleteOrder implements ActionListener{
		
			@Override
				public void actionPerformed(ActionEvent e) {
					conn = DataBaseConection.getDataBaseConection();
					System.out.println("add");
					int id3 = Integer.parseInt(JOptionPane.showInputDialog(tabs,"Enter the id for order:"));
					String sql = "delete from ORDERS where ID_ORDER = ?";
					try {
						PreparedStatement prep = conn.prepareStatement(sql);
							prep.setInt(1, id3);
							prep.execute();
							refreshContent3();
						prep.close();
						conn.close();
					} catch (SQLException e11) {
						e11.printStackTrace();
					}
				}
			}//end deleteOrder
		
}//end startGui