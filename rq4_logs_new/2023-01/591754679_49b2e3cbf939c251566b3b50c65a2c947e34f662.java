


package solitaireInterface;

import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;

import javax.imageio.ImageIO;

public class Card_Graphics extends Stock_Pile{

	private BufferedImage cardImage;
	
	Card_Graphics(int x, int y, BufferedImage image) {
		super(x, y);
		this.cardImage = image;
	}

	 @Override
	    protected void paintComponent(Graphics g) {
	        super.paintComponent(g);
	        if (cardImage != null) {
	            g.drawImage(cardImage, x, y, this);
	        }
	 }
	 
	    @Override
	    public Dimension getPreferredSize() {
	        if (cardImage == null || isPreferredSizeSet()) {
	            return super.getPreferredSize();
	        } else {
	            int w = 85;
	            int h = 119;
	            return new Dimension(w, h);
	        }
	    }
	
}