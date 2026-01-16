package ajbinky;

import javax.swing.JFrame;

public class GameBoard extends JFrame {

	private static final long serialVersionUID = 1L;
	private final int pxlSize = 32;
	//min value of 8 - idk max yet
	private int boardMod = 16;
	private int boardSize = pxlSize * boardMod;
	

	
	
	public GameBoard() {
		
		add(new Pxl());
		
		setTitle("Snake.apk");
		setSize(boardSize, boardSize);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		
	}
}