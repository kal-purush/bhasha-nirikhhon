package ui;

import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.JLabel;
import javax.swing.JPanel;


public class GameTitle extends JPanel {
	
	
	private static BufferedImage getImage(String directory){
		ClassLoader loader = GameTitle.class.getClassLoader();
				try{
					return ImageIO.read(loader.getResource(directory));
				}catch(IOException e){
					return null;
				}
	}
	
	protected JLabel pressAnyKey;
	protected BufferedImage bg = getImage("res/img/titlebg.jpg");;
	
	public GameTitle(){
		this.setPreferredSize(new Dimension(800, 600));
		
		
	}

}