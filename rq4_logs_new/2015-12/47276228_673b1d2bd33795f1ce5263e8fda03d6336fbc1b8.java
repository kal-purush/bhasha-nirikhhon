import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;


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
public class ReservationsWindow extends JFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5443741672739866227L;
	private JLabel jLabel1;
	private JComboBox jComboBox1;
	private JLabel jLabel2;
	private JLabel jLabel4;
	private JScrollPane jScrollPane1;
	private JButton jButton1;
	private JLabel jLabel5;
	private JTextArea jTextArea1;
	private JTextField jTextField2;
	private JLabel jLabel3;
	private JTextField jTextField1;
	private ComboBoxModel jComboBox1Model;
	
	private Flight theflight;
	
	public ReservationsWindow (Flight f)
	{
		theflight = f;
		this.initGUI();
	}
	private void initGUI() {
		try {
			setTitle("New reservation for flight " + theflight.FlightNumber);
			GridLayout thisLayout = new GridLayout(5, 2);
			thisLayout.setColumns(2);
			thisLayout.setHgap(5);
			thisLayout.setVgap(5);
			thisLayout.setRows(5);
			getContentPane().setLayout(thisLayout);
			{
				jLabel1 = new JLabel();
				getContentPane().add(jLabel1);
				getContentPane().add(getJComboBox1());
				{
					jLabel2 = new JLabel();
					getContentPane().add(jLabel2);
					jLabel2.setText("Meal Option");
				}
				{
					jTextField1 = new JTextField();
					getContentPane().add(jTextField1);
				}
				{
					jLabel3 = new JLabel();
					getContentPane().add(jLabel3);
					jLabel3.setText("Seat:");
				}
				{
					jTextField2 = new JTextField();
					getContentPane().add(jTextField2);
				}
				{
					jLabel4 = new JLabel();
					getContentPane().add(jLabel4);
					getContentPane().add(getJScrollPane1());
					jLabel4.setText("Notes:");
				}
				{
					jLabel5 = new JLabel();
					getContentPane().add(jLabel5);
				}
				{
					jButton1 = new JButton();
					getContentPane().add(jButton1);
					jButton1.setText("Make Reservation");
					jButton1.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent evt) {
							jButton1ActionPerformed(evt);
						}
					});
				}
				jLabel1.setText("Passenger:");
			}
			{
				this.setSize(550, 180);
				
			}
			this.pack();
			this.setLocationRelativeTo(null);
			this.setVisible(true);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private JComboBox getJComboBox1() {
		if(jComboBox1 == null) {
			jComboBox1Model = 
				new DefaultComboBoxModel(DatabaseAccess.GetCustomers());
			jComboBox1 = new JComboBox();
			jComboBox1.setModel(jComboBox1Model);
		}
		return jComboBox1;
	}
	
	private void jButton1ActionPerformed(ActionEvent evt) {
		
		Passenger p = (Passenger) this.jComboBox1.getSelectedItem();
		if (p != null)
		{
			DatabaseAccess.MakeReservation(theflight, p, 
							this.jTextField2.getText(), 
							this.jTextField1.getText(), 
							this.jTextArea1.getText());
			
			this.dispose();
		}
	}
	
	private JScrollPane getJScrollPane1() {
		if(jScrollPane1 == null) {
			jScrollPane1 = new JScrollPane();
			jScrollPane1.setPreferredSize(new java.awt.Dimension(181, 25));
			{
				jTextArea1 = new JTextArea();
				jScrollPane1.setViewportView(jTextArea1);
			}
		}
		return jScrollPane1;
	}

}