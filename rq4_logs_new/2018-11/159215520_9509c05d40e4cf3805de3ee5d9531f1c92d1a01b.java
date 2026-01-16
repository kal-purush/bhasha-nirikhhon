import java.awt.Dimension;

import javax.swing.JFrame;

public class FunGame {
	public static final int gamePanelWidth = 400;
	public static final int gamePanelHeight = 400;
	
	JFrame gameFrame = new JFrame();
	FunGamePanel gamePanel;

	public static void main(String[] args) {
		new FunGame();
	}

	public FunGame() {
		gamePanel = new FunGamePanel();
		
		gameFrame.setSize(new Dimension(gamePanelWidth + 30, gamePanelHeight + 60));
		gameFrame.add(gamePanel);
		gameFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		gameFrame.setVisible(true);
		
		gameFrame.addKeyListener(gamePanel);
	}
}