import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

public class TempConversionWindow extends JFrame{
    private Container pane;
    private JButton b, c;
    private JLabel l;
    private JTextField t;

    public TempConversionWindow() {
	this.setTitle("Temperature GUI");
	this.setSize(600,400);
	this.setLocation(100,100);
	this.setDefaultCloseOperation(EXIT_ON_CLOSE);


	pane = this.getContentPane();
	pane.setLayout(new BoxLayout(pane, BoxLayout.Y_AXIS));

	b = new JButton("Convert Celcius to Farenheit");
	b.addActionListener(this);
	b.setActionCommand("CToF");

	c = new JButton("Convert Farenheit to Celcius");
	c.addActionListener(this);
	c.setActionCommand("FToC");

	l = new JLabel("Temperature in ",null,JLabel.CENTER);
	t = new JTextField(10);
	pane.add(t);
	pane.add(b);
	pane.add(c);
	pane.add(l);
    }

    public void actionPerformed(ActionEvent e){
	String event = e.getActionCommand();
	if(event.equals("CToF")){
	    double s = Integer.parseInt(t);
	    s=CtoF(s);
	    l.setText(s);
	}
	if(event.equals("FToC")){
	    l.setText("Fish");
  
	}
    }

    public static double CtoF (double t){
	return (t*1.8)+32.0;
    }
    public static double FtoC (double t){
	return (t-32.0)*5.0/9.0;
    }

}