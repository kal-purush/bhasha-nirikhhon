
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JColorChooser;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.event.ChangeListener;


/**
 * Homework: Custom Dialog
 * Provides a custom color chooser dialog used in <code>HW2</code>..
 * 
 * @author Denis Gracanin
 * @version 1
 */
public class ColorDialog extends JDialog implements ActionListener{
	private static final long serialVersionUID = 1L;
	private JColorChooser colorChooser = null;
	private JButton quitButton = null;

	/**
	 * Creates an instance of <code>ColorDialog</code> class.
	 * 
	 * @param f The frame "owning" this dialog.
	 * @param c The current color (initially selected color in the color chooser).
	 * @param cl The change listener that is notified when the selected color changes.
	 */
	public ColorDialog(JFrame f, Color c, ChangeListener cl) {
		super(f, true);
		setLayout(new BorderLayout());
		colorChooser = new JColorChooser();
		colorChooser.getSelectionModel().addChangeListener(cl);
		colorChooser.getSelectionModel().setSelectedColor(c);
		add(colorChooser, BorderLayout.CENTER);
		quitButton = new JButton("Quit");
		quitButton.addActionListener(this);
		add(quitButton, BorderLayout.SOUTH);
		pack();
	}

	/**
	 * Processes an action event to close the dialog.
	 *
	 * @param e The action event.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		this.dispose();
	}

}