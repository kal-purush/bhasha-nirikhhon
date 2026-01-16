package GUI;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;

public class CheckMateUI extends JFrame {
	
	private ImageIcon background;
	private JLabel labelImage;
	
	public CheckMateUI() {
		this.background = new ImageIcon("src/PieceImages/checkMate.jpg");
		this.setTitle("CHECKMATE");
		this.setSize(500, 500);
		this.setLocationRelativeTo(null);
		this.labelImage = new JLabel();
		this.labelImage.setIcon(this.background);
		
		this.getContentPane().add(labelImage);
		
		
	}
	
	
	
}