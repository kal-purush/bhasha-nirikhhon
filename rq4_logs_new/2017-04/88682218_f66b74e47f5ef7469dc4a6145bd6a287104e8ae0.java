package XMLWizard;


import javax.swing.JFrame;
import javax.swing.JPanel;
import java.awt.BorderLayout;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JTable;
import javax.swing.border.TitledBorder;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableModel;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.awt.event.ActionEvent;
import javax.swing.JScrollPane;
import java.awt.GridBagLayout;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import javax.swing.JTextField;
import java.awt.Component;
import java.awt.Dimension;
import javax.swing.JCheckBox;
import javax.swing.border.EtchedBorder;
import java.awt.Color;

public class DBReportBuilder {

	private JFrame frame;
	private Instance instance;
	private JTable tbResults;
	//private final DefaultTableModel tableModel = new DefaultTableModel();
	private JTextField txTitle;
	private JTextField txColumns;
	private JEditorPane epQuery;
	private JCheckBox chckbxEveryday;
	private JCheckBox chckbxMonday;
	private JCheckBox chckbxTuesday;
	private JCheckBox chckbxWednesday;
	private JCheckBox chckbxThursday;
	private JCheckBox chckbxFriday;
	private JCheckBox chckbxSaturday;
	private JCheckBox chckbxSunday;

	/**
	 * Create the application.
	 */
	public DBReportBuilder(Instance inst) {
		this.instance=inst;
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 614, 538);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		JPanel panel = new JPanel();
		frame.getContentPane().add(panel, BorderLayout.NORTH);
		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{150, 150, 212, 0};
		gbl_panel.rowHeights = new int[]{36, 0};
		gbl_panel.columnWeights = new double[]{0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_panel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gbl_panel);

		JLabel lbDbname = new JLabel(this.instance.getDbName());
		lbDbname.setBorder(new TitledBorder(null, "Database", TitledBorder.LEADING, TitledBorder.TOP, null, null));
		lbDbname.setToolTipText("");
		GridBagConstraints gbc_lbDbname = new GridBagConstraints();
		gbc_lbDbname.fill = GridBagConstraints.HORIZONTAL;
		gbc_lbDbname.anchor = GridBagConstraints.NORTH;
		gbc_lbDbname.insets = new Insets(0, 0, 0, 5);
		gbc_lbDbname.gridx = 0;
		gbc_lbDbname.gridy = 0;
		panel.add(lbDbname, gbc_lbDbname);

		JLabel lbUsername = new JLabel(this.instance.getUserName());
		lbUsername.setBorder(new TitledBorder(null, "Username", TitledBorder.LEADING, TitledBorder.TOP, null, null));
		GridBagConstraints gbc_lbUsername = new GridBagConstraints();
		gbc_lbUsername.fill = GridBagConstraints.HORIZONTAL;
		gbc_lbUsername.anchor = GridBagConstraints.NORTH;
		gbc_lbUsername.insets = new Insets(0, 0, 0, 5);
		gbc_lbUsername.gridx = 1;
		gbc_lbUsername.gridy = 0;
		panel.add(lbUsername, gbc_lbUsername);

		JPanel panel_1 = new JPanel();
		frame.getContentPane().add(panel_1, BorderLayout.CENTER);
		GridBagLayout gbl_panel_1 = new GridBagLayout();
		gbl_panel_1.columnWidths = new int[]{285, 0};
		gbl_panel_1.rowHeights = new int[]{78, 159, 160, 0};
		gbl_panel_1.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_panel_1.rowWeights = new double[]{0.0, 0.0, 0.0, Double.MIN_VALUE};
		panel_1.setLayout(gbl_panel_1);

		JPanel panel_2 = new JPanel();
		panel_2.setMaximumSize(new Dimension(15000, 32767));
		GridBagConstraints gbc_panel_2 = new GridBagConstraints();
		gbc_panel_2.fill = GridBagConstraints.HORIZONTAL;
		gbc_panel_2.anchor = GridBagConstraints.NORTH;
		gbc_panel_2.insets = new Insets(0, 0, 5, 0);
		gbc_panel_2.gridx = 0;
		gbc_panel_2.gridy = 0;
		panel_1.add(panel_2, gbc_panel_2);
		GridBagLayout gbl_panel_2 = new GridBagLayout();
		gbl_panel_2.columnWidths = new int[]{139, 92, 0, 0, 0, 92, 0, 92, 0};
		gbl_panel_2.rowHeights = new int[] {0, 26, 0, 0};
		gbl_panel_2.columnWeights = new double[]{1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_panel_2.rowWeights = new double[]{1.0, 0.0, 1.0, Double.MIN_VALUE};
		panel_2.setLayout(gbl_panel_2);

		txTitle = new JTextField();
		txTitle.setAlignmentX(Component.LEFT_ALIGNMENT);
		txTitle.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.LOWERED, null, null), "Report Title", TitledBorder.LEADING, TitledBorder.TOP, null, new Color(0, 0, 0)));
		GridBagConstraints gbc_txTitle = new GridBagConstraints();
		gbc_txTitle.fill = GridBagConstraints.BOTH;
		gbc_txTitle.gridwidth = 8;
		gbc_txTitle.insets = new Insets(0, 0, 5, 0);
		gbc_txTitle.gridx = 0;
		gbc_txTitle.gridy = 0;
		panel_2.add(txTitle, gbc_txTitle);
		txTitle.setColumns(10);
		JScrollPane scrollPane_1 = new JScrollPane();
		GridBagConstraints gbc_scrollPane_1 = new GridBagConstraints();
		gbc_scrollPane_1.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_1.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane_1.gridx = 0;
		gbc_scrollPane_1.gridy = 1;
		panel_1.add(scrollPane_1, gbc_scrollPane_1);

		epQuery = new JEditorPane();
		epQuery.setBorder(new TitledBorder(null, "Query", TitledBorder.LEADING, TitledBorder.TOP, null, null));
		scrollPane_1.setViewportView(epQuery);

		JPanel panel_3 = new JPanel();
		GridBagConstraints gbc_panel_3 = new GridBagConstraints();
		gbc_panel_3.anchor = GridBagConstraints.NORTH;
		gbc_panel_3.gridwidth = 8;
		gbc_panel_3.insets = new Insets(0, 0, 5, 0);
		gbc_panel_3.fill = GridBagConstraints.HORIZONTAL;
		gbc_panel_3.gridx = 0;
		gbc_panel_3.gridy = 1;
		panel_2.add(panel_3, gbc_panel_3);
		GridBagLayout gbl_panel_3 = new GridBagLayout();
		gbl_panel_3.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_panel_3.rowHeights = new int[]{0, 0, 0};
		gbl_panel_3.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_panel_3.rowWeights = new double[]{0.0, 0.0, Double.MIN_VALUE};
		panel_3.setLayout(gbl_panel_3);

		chckbxEveryday = new JCheckBox("Everyday");
		GridBagConstraints gbc_chckbxEveryday = new GridBagConstraints();
		gbc_chckbxEveryday.insets = new Insets(0, 0, 5, 5);
		gbc_chckbxEveryday.gridx = 0;
		gbc_chckbxEveryday.gridy = 0;
		panel_3.add(chckbxEveryday, gbc_chckbxEveryday);


		chckbxMonday = new JCheckBox("Monday");
		GridBagConstraints gbc_chckbxMonday = new GridBagConstraints();
		gbc_chckbxMonday.insets = new Insets(0, 0, 5, 5);
		gbc_chckbxMonday.gridx = 1;
		gbc_chckbxMonday.gridy = 0;
		panel_3.add(chckbxMonday, gbc_chckbxMonday);

		chckbxTuesday = new JCheckBox("Tuesday");
		GridBagConstraints gbc_chckbxTuesday = new GridBagConstraints();
		gbc_chckbxTuesday.insets = new Insets(0, 0, 5, 5);
		gbc_chckbxTuesday.gridx = 2;
		gbc_chckbxTuesday.gridy = 0;
		panel_3.add(chckbxTuesday, gbc_chckbxTuesday);

		chckbxWednesday = new JCheckBox("Wednesday");
		GridBagConstraints gbc_chckbxWednesday = new GridBagConstraints();
		gbc_chckbxWednesday.anchor = GridBagConstraints.WEST;
		gbc_chckbxWednesday.insets = new Insets(0, 0, 5, 5);
		gbc_chckbxWednesday.gridx = 3;
		gbc_chckbxWednesday.gridy = 0;
		panel_3.add(chckbxWednesday, gbc_chckbxWednesday);

		JPanel panel_4 = new JPanel();
		GridBagConstraints gbc_panel_4 = new GridBagConstraints();
		gbc_panel_4.gridheight = 2;
		gbc_panel_4.gridwidth = 4;
		gbc_panel_4.gridx = 4;
		gbc_panel_4.gridy = 0;
		panel_3.add(panel_4, gbc_panel_4);
		GridBagLayout gbl_panel_4 = new GridBagLayout();
		gbl_panel_4.columnWidths = new int[]{0, 91, 0};
		gbl_panel_4.rowHeights = new int[]{23, 23, 0};
		gbl_panel_4.columnWeights = new double[]{0.0, 0.0, Double.MIN_VALUE};
		gbl_panel_4.rowWeights = new double[]{0.0, 0.0, Double.MIN_VALUE};
		panel_4.setLayout(gbl_panel_4);

		JButton btnCleanUp = new JButton("Clean up");
		btnCleanUp.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				clearTx();
			}
		});
		JButton btnRunQuery = new JButton("Run Query");
		GridBagConstraints gbc_btnRunQuery = new GridBagConstraints();
		gbc_btnRunQuery.insets = new Insets(0, 0, 5, 5);
		gbc_btnRunQuery.anchor = GridBagConstraints.NORTHWEST;
		gbc_btnRunQuery.gridx = 0;
		gbc_btnRunQuery.gridy = 0;
		panel_4.add(btnRunQuery, gbc_btnRunQuery);
		btnRunQuery.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				//validate db info
				try {
					Class.forName("oracle.jdbc.driver.OracleDriver");
				} catch (ClassNotFoundException e) {
					JOptionPane.showMessageDialog(null, e.getMessage(), "DbName", JOptionPane.ERROR_MESSAGE);
					e.printStackTrace();
				}
				Connection connection = null;
				//connection = DriverManager.getConnection("jdbc:oracle:thin:@"+instance.getHostName()+":"+instance.getPort()+"/"+instance.getDbName(),instance.getUserName(),decrypt(instance.getPassw()));

				try {
					connection = DriverManager.getConnection("jdbc:oracle:thin:@"+instance.getHostName()+":"+instance.getPort()+"/"+instance.getDbName(),instance.getUserName(),instance.getPassw());
				} catch (SQLException e) {
					JOptionPane.showMessageDialog(null, e.getMessage(), "DbName", JOptionPane.ERROR_MESSAGE);
					e.printStackTrace();
				}
				//+txDbname.getText()+"/"+instance.getDbName(),instance.getUserName(),decrypt(instance.getPassw()));
				try {
					Statement stmt = connection.createStatement();
					System.out.println(epQuery.getText());
					ResultSet rs=stmt.executeQuery(epQuery.getText());
					resultSetToTableModel(rs,tbResults);
					connection.close();
				} catch (SQLException e) {
					JOptionPane.showMessageDialog(null, e.getMessage(), "DbName", JOptionPane.ERROR_MESSAGE);
					e.printStackTrace();
				}
			}
		});
		GridBagConstraints gbc_btnCleanUp = new GridBagConstraints();
		gbc_btnCleanUp.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnCleanUp.insets = new Insets(0, 0, 5, 0);
		gbc_btnCleanUp.gridx = 1;
		gbc_btnCleanUp.gridy = 0;
		panel_4.add(btnCleanUp, gbc_btnCleanUp);
		
				JButton btnSaveQuery = new JButton("Save Query");
				GridBagConstraints gbc_btnSaveQuery = new GridBagConstraints();
				gbc_btnSaveQuery.fill = GridBagConstraints.HORIZONTAL;
				gbc_btnSaveQuery.anchor = GridBagConstraints.NORTH;
				gbc_btnSaveQuery.gridx = 1;
				gbc_btnSaveQuery.gridy = 1;
				panel_4.add(btnSaveQuery, gbc_btnSaveQuery);
				btnSaveQuery.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent arg0) {
						Report report = new Report();
						StringTokenizer strtk =new StringTokenizer(txColumns.getText(), ",");
						if(strtk.countTokens()==tbResults.getColumnCount()){
						//Custom button text
						Object[] options = {"Create another report",
						"Save this report and exit"};
						int n = JOptionPane.showOptionDialog(frame,
								"What Would you like to do?",
								"A Silly Question",
								JOptionPane.YES_NO_CANCEL_OPTION,
								JOptionPane.QUESTION_MESSAGE,
								null,
								options,
								options[1]);
						report.setDayOfWeek(getDayOfWeek());
						report.setTitle(txTitle.getText());
						report.setRawColname(txColumns.getText());
						//report.setQuery(report.formatQuery(epQuery.getText()));
						report.setQuery(epQuery.getText());
						if (n==0){
							instance.add(report);
							clearTx();
						} else if(n==1){
							instance.add(report);
							try {
								instance.setPassw(instance.getEncryPass());
								JFileChooser fileChooser = new JFileChooser();
								fileChooser.addChoosableFileFilter(new FileNameExtensionFilter("File XML (.xml)", "xml"));
								fileChooser.addChoosableFileFilter(new FileNameExtensionFilter("File XML (.XML)", "XML")); 
								File file;
								if (fileChooser.showSaveDialog(null) == JFileChooser.APPROVE_OPTION) {
								  file = fileChooser.getSelectedFile();
								//  file = new File("U:\\file.xml");
									JAXBContext jaxbContext = JAXBContext.newInstance(Instance.class);
									Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

									// output pretty printed
									jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

									jaxbMarshaller.marshal(instance, file);
									jaxbMarshaller.marshal(instance, System.out);
								  // save to file
								}
								

							} catch (JAXBException e) {
								e.printStackTrace();
							}
						}
					}else{
						JOptionPane.showMessageDialog(null, "Invalid Number of columns", "DbName", JOptionPane.ERROR_MESSAGE);
						return;
					}
				}	
				});
		chckbxEveryday.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent actionEvent) {
				if(chckbxEveryday.isSelected() ){
					chckbxMonday.setEnabled(false);
					chckbxFriday.setEnabled(false);
					chckbxSaturday.setEnabled(false);
					chckbxSunday.setEnabled(false);
					chckbxThursday.setEnabled(false);
					chckbxTuesday.setEnabled(false);
					chckbxWednesday.setEnabled(false);
				}else{
					chckbxMonday.setEnabled(true);
					chckbxFriday.setEnabled(true);
					chckbxSaturday.setEnabled(true);
					chckbxSunday.setEnabled(true);
					chckbxThursday.setEnabled(true);
					chckbxTuesday.setEnabled(true);
					chckbxWednesday.setEnabled(true);
				}
			}
		});

		chckbxThursday = new JCheckBox("Thursday");
		GridBagConstraints gbc_chckbxThursday = new GridBagConstraints();
		gbc_chckbxThursday.anchor = GridBagConstraints.NORTH;
		gbc_chckbxThursday.insets = new Insets(0, 0, 0, 5);
		gbc_chckbxThursday.gridx = 0;
		gbc_chckbxThursday.gridy = 1;
		panel_3.add(chckbxThursday, gbc_chckbxThursday);
		chckbxFriday = new JCheckBox("Friday");
		GridBagConstraints gbc_chckbxFriday = new GridBagConstraints();
		gbc_chckbxFriday.anchor = GridBagConstraints.NORTH;
		gbc_chckbxFriday.insets = new Insets(0, 0, 0, 5);
		gbc_chckbxFriday.gridx = 1;
		gbc_chckbxFriday.gridy = 1;
		panel_3.add(chckbxFriday, gbc_chckbxFriday);

		chckbxSaturday = new JCheckBox("Saturday");
		GridBagConstraints gbc_chckbxSaturday = new GridBagConstraints();
		gbc_chckbxSaturday.anchor = GridBagConstraints.NORTH;
		gbc_chckbxSaturday.insets = new Insets(0, 0, 0, 5);
		gbc_chckbxSaturday.gridx = 2;
		gbc_chckbxSaturday.gridy = 1;
		panel_3.add(chckbxSaturday, gbc_chckbxSaturday);

		chckbxSunday = new JCheckBox("Sunday");
		GridBagConstraints gbc_chckbxSunday = new GridBagConstraints();
		gbc_chckbxSunday.insets = new Insets(0, 0, 0, 5);
		gbc_chckbxSunday.anchor = GridBagConstraints.NORTHWEST;
		gbc_chckbxSunday.gridx = 3;
		gbc_chckbxSunday.gridy = 1;
		panel_3.add(chckbxSunday, gbc_chckbxSunday);

		txColumns = new JTextField();
		txColumns.setBorder(new TitledBorder(null, "Columns", TitledBorder.LEADING, TitledBorder.TOP, null, null));
		GridBagConstraints gbc_txColumns = new GridBagConstraints();
		gbc_txColumns.fill = GridBagConstraints.BOTH;
		gbc_txColumns.gridwidth = 8;
		gbc_txColumns.anchor = GridBagConstraints.NORTHWEST;
		gbc_txColumns.gridx = 0;
		gbc_txColumns.gridy = 2;
		panel_2.add(txColumns, gbc_txColumns);
		txColumns.setColumns(10);



		tbResults = new JTable();
		//	panel_1.add(tbResults);

		JScrollPane scrollPane = new JScrollPane(tbResults);
		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 2;
		panel_1.add(scrollPane, gbc_scrollPane);
		frame.setLocationRelativeTo(null);
		frame.setVisible(true);
	}
	public void resultSetToTableModel(ResultSet rs, JTable table) throws SQLException{
		//Create new table model
		DefaultTableModel tableModel = new DefaultTableModel();

		//Retrieve meta data from ResultSet
		ResultSetMetaData metaData = rs.getMetaData();

		//Get number of columns from meta data
		int columnCount = metaData.getColumnCount();

		//Get all column names from meta data and add columns to table model
		for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++){
			tableModel.addColumn(metaData.getColumnLabel(columnIndex));
		}

		//Create array of Objects with size of column count from meta data
		Object[] row = new Object[columnCount];

		//Scroll through result set
		while (rs.next()){
			//Get object from column with specific index of result set to array of objects
			for (int i = 0; i < columnCount; i++){
				row[i] = rs.getObject(i+1);
			}
			//Now add row to table model with that array of objects as an argument
			tableModel.addRow(row);
		}

		//Now add that table model to your table and you are done :D
		table.setModel(tableModel);
	}
	public void clearTx(){
		this.txColumns.setText("");
		this.txTitle.setText("");
		this.epQuery.setText("");
		tbResults.setModel(new DefaultTableModel());
		this.chckbxMonday.setEnabled(true);
		this.chckbxFriday.setEnabled(true);
		this.chckbxSaturday.setEnabled(true);
		this.chckbxSunday.setEnabled(true);
		this.chckbxThursday.setEnabled(true);
		this.chckbxTuesday.setEnabled(true);
		this.chckbxWednesday.setEnabled(true);
		this.chckbxEveryday.setSelected(false);
		this.chckbxMonday.setSelected(false);
		this.chckbxFriday.setSelected(false);
		this.chckbxSaturday.setSelected(false);
		this.chckbxSunday.setSelected(false);
		this.chckbxThursday.setSelected(false);
		this.chckbxTuesday.setSelected(false);
		this.chckbxWednesday.setSelected(false);
		this.chckbxEveryday.setEnabled(true);
	}
	public String getDayOfWeek(){
		String days="";
		ArrayList<String> dow=new ArrayList<String>();
		if(this.chckbxEveryday.isSelected()){
			return "Everyday";
		}else{
			if(this.chckbxMonday.isSelected()){
				dow.add("Monday");
			}
			if(this.chckbxTuesday.isSelected()){
				dow.add("Tuesday");
			}
			if(this.chckbxWednesday.isSelected()){
				dow.add("Wednesday");
			}
			if(this.chckbxThursday.isSelected()){
				dow.add("Thursday");
			}
			if(this.chckbxFriday.isSelected()){
				dow.add("Friday");
			}
			if(this.chckbxSaturday.isSelected()){
				dow.add("Saturday");
			}
			if(this.chckbxSunday.isSelected()){
				dow.add("Sunday");
			}
			for(int i=0;i<dow.size();i++){
				days=days+dow.get(i)+",";
			}
			return days.substring(0,days.length()-1);
		}
	}
}