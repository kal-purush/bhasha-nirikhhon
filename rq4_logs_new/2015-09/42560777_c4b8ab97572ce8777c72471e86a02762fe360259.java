package org.team5763;

import java.applet.Applet;
import java.awt.Color;
import java.awt.Font;
import java.awt.Frame;
import java.awt.Graphics;
import java.awt.TextArea;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.File;

import javax.imageio.ImageIO;


public class Bolt extends Applet implements WindowListener{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5459939917288326635L;

	static Bolt boltInstance;

	static NetworkListener listener;
	static JoystickHandler joystick;
	static ConfigurationManager manager;
	
	static ConsoleInterceptor console;
	static ConsoleInterceptor error;
	static TextArea diag=new TextArea("", 100, 100,TextArea.SCROLLBARS_NONE);
	
	public static void main(String[] args) {
		boltInstance=new Bolt();
		Frame w = new Frame("Team 5763 Bolt");
	    w.addWindowListener(boltInstance);
	    w.setSize(500, 200);
	    try{
	    	w.setIconImage(ImageIO.read(new File("Bolt.png")));
		}catch(Exception e){}
	    diag.setEditable(false);
	    diag.setSize(500, 200);
	    diag.setFont(new Font("Monospaced", Font.PLAIN, 14));
	    diag.setBackground(Color.darkGray);
	    diag.setForeground(Color.green);
	    diag.setRows(10);
	    w.add(diag);
	    w.setVisible(true);
		console=new ConsoleInterceptor(false);
	    System.setOut(console);
	    error=new ConsoleInterceptor(true);
	    System.setErr(error);
	    boltInstance.init();
	    boltInstance.start();
		manager=new ConfigurationManager();
		joystick=new JoystickHandler();
		listener=new NetworkListener();
	}
	public void paint(Graphics g) {
		super.paint(g);
	}

	public static void writeConsole(String s){
		if(diag.getText().equals("")){
			diag.setText(s);
		}else{
			diag.setText(diag.getText()+"\n"+s);
		}
	}
	public static void writeError(String s){
		diag.setForeground(Color.red);
		s="[error]    "+s;
		if(diag.getText().equals("")){
			diag.setText(s);
		}else{
			diag.setText(diag.getText()+"\n"+s);
		}
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {}
		diag.setForeground(Color.green);
	}
	public void windowOpened(WindowEvent e) {}
	public void windowClosing(WindowEvent e) {
		System.exit(0);
	}
	public void windowClosed(WindowEvent e) {
		System.exit(0);
	}
	public void windowIconified(WindowEvent e) {}
	public void windowDeiconified(WindowEvent e) {}
	public void windowActivated(WindowEvent e) {}
	public void windowDeactivated(WindowEvent e) {}
	
}
