import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.BorderFactory;
import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.BevelBorder;

/**
* This code was edited or generated using CloudGarden's Jigloo
* SWT/Swing GUI Builder, which is free for non-commercial
* use. If Jigloo is being used commercially (ie, by a corporation,
* company or business for any purpose whatever) then you
* should purchase a license for each developer using Jigloo.
* Please visit www.cloudgarden.com for details.
* Use of Jigloo implies acceptance of these licensing terms.
* A COMMERCIAL LICENSE HAS NOT BEEN PURCHASED FOR
* THIS MACHINE, SO JIGLOO OR THIS CODE CANNOT BE USED
* LEGALLY FOR ANY CORPORATE OR COMMERCIAL PURPOSE.
*/
public class CreateOrderWindow extends JFrame {


	/**
	 * 
	 */
	private static final long serialVersionUID = -3820743935225449137L;
	private JPanel jPanel1;
	private JLabel jLabel4;
	private JButton jButton2;
	private JPanel jPanel4;
	private JList jList1;
	private JButton jButton1;
	private JTextField jTextField1;
	private JLabel jLabel3;
	private JComboBox jComboBox2;
	private JLabel jLabel2;
	private JPanel jPanel3;
	private JComboBox jComboBox1;
	private JLabel jLabel1;
	private JPanel jPanel2;
	private DefaultComboBoxModel jList1Model;
	
	public CreateOrderWindow ()
	{
		this.initGUI();
		this.pack();
		this.setSize(700, 500);
		this.setVisible(true);
	}
	private JPanel getJPanel1() {
		if(jPanel1 == null) {
			jPanel1 = new JPanel();
			GridLayout jPanel1Layout = new GridLayout(1, 2);
			jPanel1Layout.setHgap(5);
			jPanel1Layout.setVgap(5);
			jPanel1Layout.setColumns(2);
			jPanel1.setLayout(jPanel1Layout);
			jPanel1.setBorder(BorderFactory.createBevelBorder(BevelBorder.LOWERED));
			jPanel1.add(getJLabel1());
			jPanel1.add(getJComboBox1());
		}
		return jPanel1;
	}
	
	private void initGUI() {
		try {
			getContentPane().add(getJPanel1(), BorderLayout.NORTH);
			getContentPane().add(getJPanel4(), BorderLayout.SOUTH);
			getContentPane().add(getJPanel2(), BorderLayout.CENTER);
			this.setTitle("Create new order");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private JPanel getJPanel2() {
		if(jPanel2 == null) {
			jPanel2 = new JPanel();
			BorderLayout jPanel2Layout = new BorderLayout();
			jPanel2.setLayout(jPanel2Layout);
			jPanel2.add(getJPanel3(), BorderLayout.NORTH);
			jPanel2.add(getJList1(), BorderLayout.CENTER);
		}
		return jPanel2;
	}
	
	private JLabel getJLabel1() {
		if(jLabel1 == null) {
			jLabel1 = new JLabel();
			jLabel1.setText("Customer:");
		}
		return jLabel1;
	}
	
	private JComboBox getJComboBox1() {
		if(jComboBox1 == null) {
			ComboBoxModel jComboBox1Model = 
				new DefaultComboBoxModel( DatabaseAccess.GetCustomers());
			jComboBox1 = new JComboBox();
			jComboBox1.setModel(jComboBox1Model);
		}
		return jComboBox1;
	}
	
	private JPanel getJPanel3() {
		if(jPanel3 == null) {
			jPanel3 = new JPanel();
			GridLayout jPanel3Layout = new GridLayout(3, 2);
			jPanel3Layout.setColumns(2);
			jPanel3Layout.setHgap(5);
			jPanel3Layout.setVgap(5);
			jPanel3Layout.setRows(3);
			jPanel3.setLayout(jPanel3Layout);
			jPanel3.setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
			jPanel3.add(getJLabel2());
			jPanel3.add(getJComboBox2());
			jPanel3.add(getJLabel3());
			jPanel3.add(getJTextField1());
			jPanel3.add(getJLabel4());
			jPanel3.add(getJButton1());
		}
		return jPanel3;
	}
	
	private JLabel getJLabel2() {
		if(jLabel2 == null) {
			jLabel2 = new JLabel();
			jLabel2.setText("Product:");
		}
		return jLabel2;
	}
	
	private JComboBox getJComboBox2() {
		if(jComboBox2 == null) {
			ComboBoxModel jComboBox2Model = 
				new DefaultComboBoxModel(
						DatabaseAccess.GetProducts());
			jComboBox2 = new JComboBox();
			jComboBox2.setModel(jComboBox2Model);
		}
		return jComboBox2;
	}
	
	private JLabel getJLabel3() {
		if(jLabel3 == null) {
			jLabel3 = new JLabel();
			jLabel3.setText("Qty:");
			jLabel3.setPreferredSize(new java.awt.Dimension(341, 23));
		}
		return jLabel3;
	}
	
	private JTextField getJTextField1() {
		if(jTextField1 == null) {
			jTextField1 = new JTextField();
			jTextField1.setPreferredSize(new java.awt.Dimension(104, 23));
		}
		return jTextField1;
	}
	
	private JLabel getJLabel4() {
		if(jLabel4 == null) {
			jLabel4 = new JLabel();
		}
		return jLabel4;
	}
	
	private JButton getJButton1() {
		if(jButton1 == null) {
			jButton1 = new JButton();
			jButton1.setText("Add to Order");
			jButton1.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent evt) {
					jButton1ActionPerformed(evt);
				}
			});
		}
		return jButton1;
	}
	
	private JList getJList1() {
		if(jList1 == null) {
			jList1Model = 
				new DefaultComboBoxModel();
			jList1 = new JList();
			jList1.setModel(jList1Model);
		}
		return jList1;
	}
	
	private void jButton1ActionPerformed(ActionEvent evt) {
		Product p = (Product) jComboBox2.getSelectedItem();
		if (p != null)
		{
			LineItem li = new LineItem();
			li.Product = p;
			li.Quantity = Integer.parseInt(jTextField1.getText());
			li.PricePaid = li.Product.Price;
			jList1Model.addElement(li);
		}
	}
	
	private JPanel getJPanel4() {
		if(jPanel4 == null) {
			jPanel4 = new JPanel();
			FlowLayout jPanel4Layout = new FlowLayout();
			jPanel4Layout.setAlignment(FlowLayout.RIGHT);
			jPanel4.setLayout(jPanel4Layout);
			jPanel4.setPreferredSize(new java.awt.Dimension(692, 37));
			jPanel4.add(getJButton2());
		}
		return jPanel4;
	}
	
	private JButton getJButton2() {
		if(jButton2 == null) {
			jButton2 = new JButton();
			jButton2.setText("Create Order");
			jButton2.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent evt) {
					jButton2ActionPerformed(evt);
				}
			});
		}
		return jButton2;
	}
	
	private void jButton2ActionPerformed(ActionEvent evt) {
	
		Customer c = (Customer) jComboBox1.getSelectedItem();
		LineItem [] LineItems = new LineItem [jList1Model.getSize()];
		for (int i=0;i<LineItems.length;i++)
		{
			LineItems[i]=(LineItem)jList1Model.getElementAt(i);			
		}
		
		DatabaseAccess.MakeOrder(c, LineItems);
		
		this.dispose();
	}

}