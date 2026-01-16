import java.awt.Button;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.Label;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;

public class GatherEvidence extends Frame implements ActionListener, WindowListener {
	// This is where I will put the beginner gui...when I get some free time
	private Label lblCount; // Declare component Label
	private TextField tfCount; // Declare component TextField
	private Button btnCount; // Declare component Button
	private int count = 0; // Counter's value

	/** Constructor to setup GUI components and event handling */
	public GatherEvidence() {
		setLayout(new FlowLayout());
		// "super" Frame sets its layout to FlowLayout, which arranges the
		// components
		// from left-to-right, and flow to next row from top-to-bottom.

		lblCount = new Label("URL:"); // construct Label
		add(lblCount); // "super" Frame adds Label

		tfCount = new TextField("paste or type url here"); // construct TextField
		tfCount.setEditable(true); // set to read-only
		add(tfCount); // "super" Frame adds tfCount

		btnCount = new Button("Gather Evidence"); // construct Button
		add(btnCount); // "super" Frame adds Button

		btnCount.addActionListener(this);
		// Clicking Button source fires ActionEvent
		// btnCount registers this instance as ActionEvent listener

		setTitle("PhishHook"); // "super" Frame sets title
		setSize(250, 100); // "super" Frame sets initial window size
		
		addWindowListener(this);

		// System.out.println(this);
		// System.out.println(lblCount);
		// System.out.println(tfCount);
		// System.out.println(btnCount);

		setVisible(true); // "super" Frame shows

		// System.out.println(this);
		// System.out.println(lblCount);
		// System.out.println(tfCount);
		// System.out.println(btnCount);
	}

	/** The entry main() method */
	public static void main(String[] args) {
		// Invoke the constructor to setup the GUI, by allocating an instance
		GatherEvidence app = new GatherEvidence();
	}

	/** ActionEvent handler - Called back upon button-click. */
	@Override
	public void actionPerformed(ActionEvent evt) {
		//++count; // increase the counter value
		// Display the counter value on the TextField tfCount
		GatherEvidence obj = new GatherEvidence();
		System.out.println(obj.getWhois("mkyong.com"));
		//tfCount.setText(count + ""); // convert int to String
	}
	
	
	/* This method is for closing the gui */
	public void windowClosing(WindowEvent e) {
		System.exit(0);
	}

	@Override
	public void windowActivated(WindowEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void windowClosed(WindowEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void windowDeactivated(WindowEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void windowDeiconified(WindowEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void windowIconified(WindowEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void windowOpened(WindowEvent e) {
		// TODO Auto-generated method stub
		
	}
}