package h11;

import java.awt.*;
import java.applet.*;

@SuppressWarnings("serial")
public class h11_9 extends Applet {
	
    public void init() {
    }
    
    
	    public void paint(Graphics g) {
	    	
	    	int x = 10;
	    	int y = 10;
	    	int grootte = 10;

	    	for(int i = 0; i < 8; i++) { // HORIZONTAL
	    		for(int j = 0; j < 8; j++) {
	    			if((i + j) % 2 == 0) {
		    			g.setColor(Color.BLACK);
		    			g.fillRect(x, y, grootte, grootte);
		    		}
		    			
		    		else {
		    			g.setColor(Color.WHITE);
			    		g.fillRect(x, y, grootte, grootte);
		    		}
	    			x += 10;
	    		}
	    		y += 10;
	    		x = 10;
	    		
	    		
	    		
	    	}

	    }
    

}