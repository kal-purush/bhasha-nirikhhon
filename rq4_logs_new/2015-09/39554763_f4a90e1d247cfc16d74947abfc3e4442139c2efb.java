package UserInterface;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.TextArea;
import java.awt.Toolkit;
import java.util.List;

import javax.swing.BoxLayout;
import javax.swing.DefaultCellEditor;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.RowFilter;
import javax.swing.border.MatteBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import DataBaseInterface.Customer;
import DataBaseInterface.CustomerVehicle;
import DataBaseInterface.Handler;
import DataBaseInterface.ServiceBill;
import DataBaseInterface.ServiceDetail;
import DataBaseInterface.ServiceParticulars;

public class pnl_wheels_view extends JPanel {

	private JTable tbl_service_details;

	private TableRowSorter<TableModel> rowSorter ;

	private JTextField txt_search_word;
	private DefaultTableModel mdl_service_details;
	private Handler dbh = Handler.getInstance();

	private static SessionFactory factory;

	public pnl_wheels_view() {
		setBackground(Color.WHITE);

		Toolkit tool=Toolkit.getDefaultToolkit();
		Dimension dim=tool.getScreenSize();
		setSize(dim.width,dim.height);
		setLayout(null);

		// TODO Auto-generated method stub
		try{
			factory = new Configuration().configure().buildSessionFactory();
		}catch (Throwable ex) {
			System.err.println("Failed to create sessionFactory object." + ex);
			throw new ExceptionInInitializerError(ex);
		}

		tbl_service_details =  new JTable();
		mdl_service_details = new DefaultTableModel();

		String[] columns =  {
				"S.NO",
				"Name",
				"Vehicle Number",
				"Comments",
				"Service Name",
				"Quantity",
				"Amount",
				"Total"
		};

		mdl_service_details.setColumnIdentifiers(columns);

		int serial_number = 0, sp_number = 0;

		String name = "", sp_number_string;

		Object[] o = new Object[8];

		List<Object[]> bill_details = dbh.get_bill_details();

		for (Object[] object : bill_details){

			ServiceDetail      obj_sd 	= (ServiceDetail) 		object[0];
			ServiceBill        obj_sb 	= (ServiceBill)   		object[1];
			ServiceParticulars obj_sp 	= (ServiceParticulars) 	object[2];
			CustomerVehicle    obj_cveh = (CustomerVehicle) 	object[3];
			Customer           obj_cus 	= (Customer) 			object[4];

			sp_number++;
			sp_number_string = sp_number + ". ";

			//use id instead of name
			if(name.compareTo(obj_cus.getName()) == 0) {

				o[4] =  o[4] + "\n" +  sp_number_string + obj_sp.getService_name();
				o[5] =  o[5] + "\n" +  sp_number_string + obj_sd.getQuantity();
				o[6] =  o[6] + "\n" +  sp_number_string + obj_sd.getAmount();

			} else {

				if (name != "") {
					mdl_service_details.addRow(o);
					o = new Object[8];
					sp_number = 1;
					sp_number_string = sp_number + ". ";

				}

				name = obj_cus.getName();
				o[0] = ++serial_number;
				o[1] = obj_cus.getName();
				o[2] = obj_cveh.getVehicle_number();
				o[3] = obj_sb.getComments();
				o[4] = sp_number_string + obj_sp.getService_name();
				o[5] = sp_number_string + obj_sd.getQuantity();
				o[6] = sp_number_string + obj_sd.getAmount();
				o[7] = obj_sb.getTotal_amount();

			}

		}

		tbl_service_details.setModel(mdl_service_details);
		tbl_service_details.setFont(new Font("Arial", Font.PLAIN, 14));
		tbl_service_details.setForeground(Color.DARK_GRAY);
		tbl_service_details.setGridColor(Color.BLUE);
		tbl_service_details.setBackground(Color.WHITE);
		tbl_service_details.setRowMargin(5);
		tbl_service_details.setRowHeight(60);

		tbl_service_details.getColumn("Service Name").setCellRenderer(new TextAreaRenderer());
		tbl_service_details.getColumn("Service Name").setCellEditor(new TextAreaEditor());

		tbl_service_details.getColumn("Quantity").setCellRenderer(new TextAreaRenderer());
		tbl_service_details.getColumn("Quantity").setCellEditor(new TextAreaEditor());

		tbl_service_details.getColumn("Amount").setCellRenderer(new TextAreaRenderer());
		tbl_service_details.getColumn("Amount").setCellEditor(new TextAreaEditor());


		JTableHeader  obj_jtable_header = tbl_service_details.getTableHeader();
		obj_jtable_header.setBackground(Color.BLUE);
		obj_jtable_header.setForeground(Color.WHITE);
		obj_jtable_header.setFont(new Font("Bitstream Charter", Font.BOLD | Font.ITALIC, 18));

		JScrollPane jsp_service_details =new JScrollPane(tbl_service_details);
		jsp_service_details.setForeground(new Color(102, 153, 255));
		jsp_service_details.setBorder(new MatteBorder(3, 3, 3, 3, (Color) new Color(204, 51, 102)));
		jsp_service_details.setBackground(new Color(204, 102, 153));
		jsp_service_details.setBounds(74, 153, 1217, 359);
		add(jsp_service_details);

		rowSorter = new TableRowSorter<>(tbl_service_details.getModel());
		tbl_service_details.setRowSorter(rowSorter);

		JPanel pnl_search = new JPanel();
		pnl_search.setBackground(new Color(204, 51, 153));
		pnl_search.setBorder(new MatteBorder(3, 3, 3, 3, (Color) new Color(204, 51, 153)));
		pnl_search.setBounds(74, 123, 1217, 30);
		add(pnl_search);
		pnl_search.setLayout(new BoxLayout(pnl_search, BoxLayout.X_AXIS));

		JLabel lbl_search_word = new JLabel("Search Text        ");
		lbl_search_word.setBackground(new Color(204, 153, 0));
		lbl_search_word.setAlignmentX(Component.CENTER_ALIGNMENT);
		pnl_search.add(lbl_search_word);
		lbl_search_word.setForeground(new Color(255, 255, 153));
		lbl_search_word.setFont(new Font("Bitstream Charter", Font.BOLD | Font.ITALIC, 18));

		txt_search_word = new JTextField();
		txt_search_word.setBackground(new Color(255, 255, 255));
		pnl_search.add(txt_search_word);
		txt_search_word.setColumns(10);

		txt_search_word.getDocument().addDocumentListener(new DocumentListener(){
			@Override
			public void insertUpdate(DocumentEvent e) {
				String text = txt_search_word.getText();
				if (text.trim().length() == 0) {
					rowSorter.setRowFilter(null);
				} else {
					rowSorter.setRowFilter(RowFilter.regexFilter("(?i)" + text));
				}
			}

			@Override
			public void removeUpdate(DocumentEvent e) {
				String text = txt_search_word.getText();

				if (text.trim().length() == 0) {
					rowSorter.setRowFilter(null);
				} else {
					rowSorter.setRowFilter(RowFilter.regexFilter("(?i)" + text));
				}
			}

			@Override
			public void changedUpdate(DocumentEvent e) {
				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}

		});

	}

}
class TextAreaRenderer extends JScrollPane implements TableCellRenderer
{
	JTextArea textarea;

	public TextAreaRenderer() {
		textarea = new JTextArea();
		textarea.setLineWrap(true);
		textarea.setWrapStyleWord(true);
		getViewport().add(textarea);
	}

	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus,
			int row, int column)
	{
		if (isSelected) {
			setForeground(table.getSelectionForeground());
			setBackground(table.getSelectionBackground());
			textarea.setForeground(table.getSelectionForeground());
			textarea.setBackground(table.getSelectionBackground());
		} else {
			setForeground(table.getForeground());
			setBackground(table.getBackground());
			textarea.setForeground(table.getForeground());
			textarea.setBackground(table.getBackground());
		}

		textarea.setText(value.toString());
		textarea.setCaretPosition(0);
		return this;
	}
}
class TextAreaEditor extends DefaultCellEditor {
	protected JScrollPane scrollpane;
	protected JTextArea textarea;

	public TextAreaEditor() {
		super(new JCheckBox());
		scrollpane = new JScrollPane();
		textarea = new JTextArea(); 
		textarea.setLineWrap(true);
		textarea.setWrapStyleWord(true);

		scrollpane.getViewport().add(textarea);
	}

	public Component getTableCellEditorComponent(JTable table, Object value,
			boolean isSelected, int row, int column) {
		textarea.setText((String) value);

		return scrollpane;
	}

	public Object getCellEditorValue() {
		return textarea.getText();
	}
}