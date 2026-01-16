package test;

import javax.swing.*;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;



public class KiloConvertHW extends JFrame {
	
	private JPanel panel;             // To reference a panel
	private JLabel messageLabel;      // To reference a label
	private JLabel messageLabel2;
	private JTextField kiloTextField; // To reference a text field
	private JButton calcButton;       // To reference a button
	private final int WINDOW_WIDTH = 500;  // Window width
	private final int WINDOW_HEIGHT = 300; // Window height

   /**
    *  Constructor
    */

	   public KiloConvertHW() {
		   // Set Call the JFrame constructor.
		   super("TWO Kilometer Converter");

		   // Set the size of the window.
		   setSize(WINDOW_WIDTH, WINDOW_HEIGHT);

		   // Specify what happens when the close button is clicked.
		   setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		   // Build the panel and add it to the frame.
		   buildPanel();

		   // Add the panel to the frame's content pane.
		   add(panel);

		   // Display the window.
		   setVisible(true);
	   }

	   /**
	    *  The buildPanel method adds a label, text field, and
	    *  and a button to a panel.
	    */

	   private void buildPanel() {
	      // Create a label to display instructions.
	      messageLabel = new JLabel("Enter a distance in kilometers");
	      messageLabel.setForeground(Color.BLUE);

	      // Create a text field 10 characters wide.
	      kiloTextField = new JTextField(10);

	      // Create a button with the caption "Calculate".
	      calcButton = new JButton("Calculate");

	      // Add an action listener to the button.
	      calcButton.addActionListener(new CalcButtonListener());
	      
	      //Change color of the button
	      calcButton.setBackground(Color.YELLOW);
	      

	      // Create a JPanel object and let the panel
	      // field reference it.
	      panel = new JPanel();

	      // Add the label, text field, and button
	      // components to the panel.
	      panel.add(messageLabel);
	      panel.add(kiloTextField);
	      panel.add(calcButton);
	      panel.setBackground(Color.MAGENTA);
	      
	   }

	   /**
	      CalcButtonListener is an action listener class for
	      the Calculate button.
	   */

	   private class CalcButtonListener implements ActionListener  {
	      /**
	         The actionPerformed method executes when the user
	         clicks on the Calculate button.
	         @param e The event object.
	      */

	      public void actionPerformed(ActionEvent e)  {
	         final double CONVERSION = 0.6214*2;
	         String input;  // To hold the user's input
	         double miles;  // The number of miles

	         // Get the text entered by the user into the
	         // text field.
	         input = kiloTextField.getText();

	         // Convert the input to miles.
	         miles = Double.parseDouble(input) * CONVERSION;

	         // Display the result.
	         //JOptionPane.showMessageDialog(null, input +
	         //         " kilometers is " + miles + " miles.");
	         messageLabel2 = new JLabel(input + " kilometers is " + miles + " miles.");
	         panel.add(messageLabel2);
	      }
	   }
	   
	   /**
	    *  main method
	    */
	   
	   public static void main(String[] args)
	   {
	      new KiloConvertHW();
	   }


}