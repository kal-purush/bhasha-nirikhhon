package h12;

import java.awt.*;
import java.applet.*;

@SuppressWarnings("serial")
public class h12_1 extends Applet {
	
	int[] getallen = { 2, 3, 56, 123, 62, 63, 123, 11, 123, 115 };
	double gemiddelde;
	
	
    public void init() {
    }

    public void paint(Graphics g) {
    	
    	for(int i = 0; i < getallen.length; i++) {
    		g.drawString("" + getallen[i], 10, 30+(i*15));
    		gemiddelde += getallen[i];
    	}
    	gemiddelde /= getallen.length;
    	g.drawString("Gemiddelde is: " + gemiddelde, 50, 40);
   	
    }

}