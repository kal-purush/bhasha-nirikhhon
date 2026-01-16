import java.awt.Color;

import javax.swing.JFrame;
import javax.swing.JPanel;

import menu.OmokMenuBar;
import omokBaord.OmokBoard;
import omokBaord.OmokBoardInfo;
import omokText.OmokText;


@SuppressWarnings("serial")
public class OmokFrame extends JFrame{
	
	OmokMenuBar menuBar = new OmokMenuBar();	
	JPanel gamePanel = new JPanel();
	OmokText textPanel = new OmokText();		

	public OmokFrame() {
		super();
		initialize();
	}

	private void initialize() {
		gamePanel.add(OmokBoardInfo.getInstance().getOmokBoard()	);

		setTitle("Omok");
		setBackground(Color.lightGray);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setJMenuBar(menuBar);
		add(gamePanel, "West");
		add(textPanel, "East");
		setSize((int) (OmokBoard.WIDTH + 130), (int) (OmokBoard.HEIGHT + 60));
		setResizable(false);
		setVisible(true);
	}
}