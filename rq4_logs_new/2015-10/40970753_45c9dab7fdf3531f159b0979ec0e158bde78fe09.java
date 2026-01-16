package h12;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.applet.*;

@SuppressWarnings("serial")
public class h12_5 extends Applet {
	
    double[] getal = {0, 1, 2, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6, 6, 7, 8, 8, 8, 9, 9, 9, 9, 9, 9, 9, 9, 10, 10};
    boolean gevonden;
    int index = -1;
    String tekst = "Tik een fucking getal in"; 
    TextField tekstvak = new TextField("Tik een fucking getal in", 20);
	
    public void init() {
    	setSize(500, 500);
        gevonden = false;
        tekstvak.addActionListener(new OkListener());
        add(tekstvak);       
     }

    public void paint(Graphics g) {
            g.drawString("" + tekst, 20, 50);
    }
    
	class OkListener implements ActionListener {
		public void actionPerformed(ActionEvent a) {
    		int tempgetal = Integer.parseInt(tekstvak.getText());
    		for(int i = 0; i < getal.length && gevonden == false; i++) {
    			index++;
    			if(tempgetal == getal[i]) {
    				gevonden = true;
    			}
            }
    		if(gevonden == true) {
    			tekst = "Waarde " + tempgetal + " is gevonden onder index " + index;
    		}
    		else {
    			tekst = "Waarde is niet gevonden";
    		}
    		repaint();
    		gevonden = false;
    		index = -1;
    	}
    }
}