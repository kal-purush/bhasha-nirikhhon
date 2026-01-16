package animation;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.Ellipse2D;
import java.util.Vector;
import javax.swing.JComponent;
import javax.swing.JFrame;


public class function extends JComponent implements Runnable{

	private static final long serialVersionUID = 1L;
	static JFrame frame = new JFrame("Animation");
	static Vector<Double> Y = new Vector<Double>();
	double X =0;
	static double a =0.5;
	static double b = 0.1;
	static int range = 701;
	static double sample = 0.01;
	public static void main(String args[]) {
		for(int i=0;i<range;i++) {
			Y.add((double) 0);
		}
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setBounds(0,0,800,800);
		frame.getContentPane().add(new function());
		frame.getContentPane().setBackground(Color.BLACK);
		frame.setVisible(true);
	}
	
	public void paint(Graphics g)
	{
		Graphics2D g2D = (Graphics2D) g;
		g2D.translate(frame.getWidth()/2, frame.getHeight()/2);		
		RenderingHints rh = new RenderingHints(RenderingHints.KEY_ANTIALIASING,RenderingHints.VALUE_ANTIALIAS_ON);
    	rh.put(RenderingHints.KEY_RENDERING,RenderingHints.VALUE_RENDER_QUALITY);
    	g2D.setRenderingHints(rh);
		g2D.setColor(Color.WHITE);
		for(int i=0;i<range;i++) {	
			Y.set(i, calculate(i));
			g2D.fill(new Ellipse2D.Double(X-0.5,Y.elementAt(i)-0.5, 1, 1));
		}
		b+=0.001;
		run();
		//repaint();
	}
	
	private double calculate(double i) {
		X = i - range/2 + i*sample;
		double sum=0;
		for(int j=0;j<100;j++) {
			sum+= Math.pow(a, j)*Math.cos(Math.PI*X*Math.pow(b, j));
		}
		return 100*sum;
	}

	@Override
	public void run() {
		try {
			Thread.sleep(10);
			repaint();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}