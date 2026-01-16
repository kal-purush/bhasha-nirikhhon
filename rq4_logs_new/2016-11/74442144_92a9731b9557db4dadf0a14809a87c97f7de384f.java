package battleship;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.KeyStroke;
import java.awt.event.KeyEvent;
import java.awt.event.InputEvent;
import javax.swing.JLabel;
import java.awt.Panel;
import javax.swing.JTabbedPane;
import javax.swing.JTextPane;
import javax.swing.JSeparator;
import javax.swing.JToolBar;
import javax.swing.border.TitledBorder;
import javax.swing.UIManager;
import java.awt.Color;
import javax.swing.JTable;
import javax.swing.JPanel;
import javax.swing.border.BevelBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;
import java.awt.Component;
import java.awt.Label;
import javax.swing.border.LineBorder;
import java.awt.SystemColor;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import javax.swing.JButton;
import javax.swing.JComponent;

import java.awt.Button;
import java.awt.Font;
import javax.swing.border.CompoundBorder;
import javax.swing.SwingConstants;
import javax.swing.JComboBox;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JLayeredPane;
import org.eclipse.wb.swing.FocusTraversalOnArray;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import javax.swing.ImageIcon;
import java.awt.event.MouseMotionAdapter;

public class battleshipClient {

	// create variables for grids and buttons
	JTextArea ChatArea;
	public GridValue[][] OppGrid;
	public JButton[][] OppButtons;
	public GridValue[][] PlayerGrid;
	public JButton[][] PlayerButtons;
	public JButton[] shipButtons = new JButton[5];
	
	
	// grid values
	private enum GridValue {
		CARRIER (0), 
		BATTLESHIP (1), 
		CRUISER (2), 
		SUBMARINE (3), 
		DESTROYER (4), 
		EMPTY (5), 
		HIT (6),
		SELECTED (7);
		
		private final int num;
		GridValue(int num){
			this.num = num;
		}
	}
	
	// enum for GridColors
	public enum GridColor {
		EMPTY (new Color(0x1C6BA0)),
		HIT (Color.RED),
		SELECTED (SystemColor.LIGHT_GRAY);
		
		private final Color c;
		GridColor(Color c){
			this.c =c;
		}
	}
	
	// Game states
	private enum GameState {
		NOGAME,
		SETUP,
		PLAY,
		GAMEOVER, WAIT
	}
	
	private enum Orientation {
		HORIZONTAL,
		VERTICAL
	}
	
	// create state variables for game play
	private GameState gameState = GameState.NOGAME;
	private GridValue placeState = GridValue.EMPTY;
	private Orientation placeOrient = Orientation.VERTICAL;
	private JFrame frame;
	private JTextField ChatTextField;
	private JLabel GameStateText;
	private JMenuItem mntmStartGame;
	private JMenuItem mntmQuitGame;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					battleshipClient window = new battleshipClient();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public battleshipClient() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setResizable(false);
		frame.setBounds(100, 100, 685, 636);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);
		
		Label PlayersLabel = new Label("Players");
		PlayersLabel.setFont(new Font("Dialog", Font.BOLD, 12));
		PlayersLabel.setBounds(12, 2, 50, 11);
		frame.getContentPane().add(PlayersLabel);
		
		JPanel PlayersPanel = new JPanel();
		PlayersPanel.setName("");
		PlayersPanel.setBorder(new LineBorder(SystemColor.inactiveCaption, 1, true));
		PlayersPanel.setBounds(2, 11, 280, 569);
		frame.getContentPane().add(PlayersPanel);
		PlayersPanel.setLayout(null);
		
		Label PlayerLabel = new Label("Player");
		PlayerLabel.setFont(new Font("Dialog", Font.BOLD, 12));
		PlayerLabel.setBounds(15, 284, 44, 16);
		PlayersPanel.add(PlayerLabel);
		
		JPanel PlayerPanel = new JPanel();
		PlayerPanel.setLayout(null);
		PlayerPanel.setName("");
		PlayerPanel.setBorder(new LineBorder(SystemColor.inactiveCaption, 1, true));
		PlayerPanel.setBounds(4, 294, 272, 270);
		PlayersPanel.add(PlayerPanel);
		
		JPanel PlayerGridPanel = new JPanel();
		PlayerGridPanel.setForeground(Color.WHITE);
		PlayerGridPanel.setBackground(SystemColor.window);
		PlayerGridPanel.setBounds(10, 10, 250, 250);
		PlayerPanel.add(PlayerGridPanel);
		PlayerGridPanel.setLayout(new GridLayout(10, 10, 0, 0));
		
		
		// ====================================================================================================================	
		// create buttons and grid for player
		// ====================================================================================================================	
		
		PlayerGrid = new GridValue[10][10];
		PlayerButtons = new JButton[10][10];
		for (int i = 0; i < 10; i++){
			for (int j = 0; j < 10; j++){
				PlayerGrid[i][j] = GridValue.EMPTY;
				JButton b = new JButton(i+""+j);
				PlayerButtons[i][j] = b;
				PlayerButtons[i][j].setEnabled(false);
				PlayerButtons[i][j].setBorder(new LineBorder(new Color(0, 0, 0)));
				//PlayerButtons[i][j].setBorder(new CompoundBorder(new LineBorder(new Color(0, 0, 0)), new LineBorder(new Color(255, 255, 255), 2)));
				PlayerButtons[i][j].setBackground(SystemColor.window);
				
				// create player mouse listener for different states
				PlayerButtons[i][j].addMouseListener(new java.awt.event.MouseAdapter(){
					public void mouseEntered(java.awt.event.MouseEvent evt){
						switch (gameState){
						case NOGAME:
							break;
						case SETUP:
							// which
							switch (placeState){
							case CARRIER:
								shipOver(b,5);
								break;
							case BATTLESHIP:
								shipOver(b,4);
								break;
							case CRUISER:
								shipOver(b,3);
								break;
							case DESTROYER:
								shipOver(b,2);
								break;
							case SUBMARINE:
								shipOver(b,3);
								break;
							default:
								break;
							}
							break;
						case PLAY:
							break;
						case GAMEOVER:
							break;
						}
					}
					public void mouseExited(java.awt.event.MouseEvent evt){
						switch (gameState){
						case NOGAME:
							break;
						case SETUP:
							draw();
							break;
						case PLAY:
							break;
						case GAMEOVER:
							break;
						}
					}
					
					public void mousePressed(java.awt.event.MouseEvent evt){
						switch (gameState){
						case NOGAME:
							break;
						case SETUP:
							// which
							switch (placeState){
							case CARRIER:
								shipSet(b,5);
								break;
							case BATTLESHIP:
								shipSet(b,4);
								break;
							case CRUISER:
								shipSet(b,3);
								break;
							case DESTROYER:
								shipSet(b,2);
								break;
							case SUBMARINE:
								shipSet(b,3);
								break;
							default:
								break;
							}
							break;
						case PLAY:
							break;
						case GAMEOVER:
							break;
						}
					}
				});
				PlayerGridPanel.add(PlayerButtons[i][j]);
			}
		}
		
		
		
		Label OpponentLabel = new Label("Opponent");
		OpponentLabel.setFont(new Font("Dialog", Font.BOLD, 12));
		OpponentLabel.setBounds(12, 6, 62, 15);
		PlayersPanel.add(OpponentLabel);
		
		JPanel OpponentPanel = new JPanel();
		OpponentPanel.setBounds(4, 14, 272, 270);
		PlayersPanel.add(OpponentPanel);
		OpponentPanel.setLayout(null);
		OpponentPanel.setName("");
		OpponentPanel.setBorder(new LineBorder(SystemColor.inactiveCaption, 1, true));
		
		JPanel OpponentGridPanel = new JPanel();
		OpponentGridPanel.setForeground(Color.WHITE);
		OpponentGridPanel.setBackground(SystemColor.window);
		OpponentGridPanel.setBounds(10, 10, 250, 250);
		OpponentPanel.add(OpponentGridPanel);
		OpponentGridPanel.setLayout(new GridLayout(10, 10, 0, 0));
		
		Label GameStateLabel = new Label("Game State");
		GameStateLabel.setBounds(293, 3, 70, 15);
		frame.getContentPane().add(GameStateLabel);
		GameStateLabel.setFont(new Font("Dialog", Font.BOLD, 12));
		
		JPanel GameStatePanel = new JPanel();
		GameStatePanel.setLayout(null);
		GameStatePanel.setName("");
		GameStatePanel.setBorder(new LineBorder(SystemColor.inactiveCaption, 1, true));
		GameStatePanel.setBounds(285, 11, 389, 63);
		frame.getContentPane().add(GameStatePanel);
		
		GameStateText = new JLabel("Start a NEW GAME");
		GameStateText.setForeground(Color.GRAY);
		GameStateText.setFont(new Font("Tahoma", Font.BOLD, 11));
		GameStateText.setHorizontalTextPosition(SwingConstants.CENTER);
		GameStateText.setHorizontalAlignment(SwingConstants.CENTER);
		GameStateText.setAlignmentX(Component.CENTER_ALIGNMENT);
		GameStateText.setBounds(10, 11, 369, 41);
		GameStatePanel.add(GameStateText);
		
		Label GamePieceLabel = new Label("Ships");
		GamePieceLabel.setBounds(293, 75, 37, 15);
		frame.getContentPane().add(GamePieceLabel);
		GamePieceLabel.setFont(new Font("Dialog", Font.BOLD, 12));
		
		JPanel GamePiecePanel = new JPanel();
		GamePiecePanel.setLayout(null);
		GamePiecePanel.setName("");
		GamePiecePanel.setBorder(new LineBorder(SystemColor.inactiveCaption, 1, true));
		GamePiecePanel.setBounds(285, 82, 389, 213);
		frame.getContentPane().add(GamePiecePanel);
		
		
		// ====================================================================================================================	
		// create buttons for placing ships
		// ====================================================================================================================	
		
		
		JButton btnCarrier = new JButton("Place Carrier");
		btnCarrier.setEnabled(false);
		btnCarrier.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (placeState != GridValue.CARRIER){
					placeOrient = Orientation.HORIZONTAL;
					GameStateText.setText("Place CARRIER HORIZONTAL (Click again for VERTICAL)");
				}
				else {
					if (placeOrient == Orientation.HORIZONTAL){
						placeOrient = Orientation.VERTICAL;
						GameStateText.setText("Place CARRIER VERTICAL (Click again for VERTICAL)");
					}else{
						placeOrient = Orientation.HORIZONTAL;
						GameStateText.setText("Place CARRIER HORIZONTAL (Click again for HORIZONTAL)");
					}
				}
				placeState = GridValue.CARRIER;
			}
		});
		btnCarrier.setBounds(32, 25, 145, 23);
		GamePiecePanel.add(btnCarrier);
		shipButtons[0] = btnCarrier;
		
		JButton btnBattleship = new JButton("Place Battleship");
		btnBattleship.setEnabled(false);
		btnBattleship.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (placeState != GridValue.BATTLESHIP){
					placeOrient = Orientation.HORIZONTAL;
					GameStateText.setText("Place BATTLESHIP HORIZONTAL (Click again for VERTICAL)");
				}
				else {
					if (placeOrient == Orientation.HORIZONTAL){
						placeOrient = Orientation.VERTICAL;
						GameStateText.setText("Place BATTLESHIP VERTICAL (Click again for VERTICAL)");
					}else{
						placeOrient = Orientation.HORIZONTAL;
						GameStateText.setText("Place BATTLESHIP HORIZONTAL (Click again for HORIZONTAL)");
					}
				}
				placeState = GridValue.BATTLESHIP;
			}
		});
		btnBattleship.setBounds(32, 59, 145, 23);
		GamePiecePanel.add(btnBattleship);
		shipButtons[1] = btnBattleship;
		
		JButton btnCruiser = new JButton("Place Cruiser");
		btnCruiser.setEnabled(false);
		btnCruiser.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (placeState != GridValue.CRUISER){
					placeOrient = Orientation.HORIZONTAL;
					GameStateText.setText("Place CRUISER HORIZONTAL (Click again for VERTICAL)");
				}
				else {
					if (placeOrient == Orientation.HORIZONTAL){
						placeOrient = Orientation.VERTICAL;
						GameStateText.setText("Place CRUISER VERTICAL (Click again for VERTICAL)");
					}else{
						placeOrient = Orientation.HORIZONTAL;
						GameStateText.setText("Place CRUISER HORIZONTAL (Click again for HORIZONTAL)");
					}
				}
				placeState = GridValue.CRUISER;
			}
		});
		btnCruiser.setBounds(32, 93, 145, 23);
		GamePiecePanel.add(btnCruiser);
		shipButtons[2] = btnCruiser;
		
		JButton btnSubmarine = new JButton("Place Submarine");
		btnSubmarine.setEnabled(false);
		btnSubmarine.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (placeState != GridValue.SUBMARINE){
					placeOrient = Orientation.HORIZONTAL;
					GameStateText.setText("Place SUBMARINE HORIZONTAL (Click again for VERTICAL)");
				}
				else {
					if (placeOrient == Orientation.HORIZONTAL){
						placeOrient = Orientation.VERTICAL;
						GameStateText.setText("Place SUBMARINE VERTICAL (Click again for VERTICAL)");
					}else{
						placeOrient = Orientation.HORIZONTAL;
						GameStateText.setText("Place SUBMARINE HORIZONTAL (Click again for HORIZONTAL)");
					}
				}
				placeState = GridValue.SUBMARINE;
			}
		});
		btnSubmarine.setBounds(32, 127, 145, 23);
		GamePiecePanel.add(btnSubmarine);
		shipButtons[3] = btnSubmarine;
		
		JButton btnDestroyer = new JButton("Place Destroyer");
		btnDestroyer.setEnabled(false);
		btnDestroyer.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (placeState != GridValue.DESTROYER){
					placeOrient = Orientation.HORIZONTAL;
					GameStateText.setText("Place DESTROYER HORIZONTAL (Click again for VERTICAL)");
				}
				else {
					if (placeOrient == Orientation.HORIZONTAL){
						placeOrient = Orientation.VERTICAL;
						GameStateText.setText("Place DESTROYER VERTICAL (Click again for VERTICAL)");
					}else{
						placeOrient = Orientation.HORIZONTAL;
						GameStateText.setText("Place DESTROYER HORIZONTAL (Click again for HORIZONTAL)");
					}
				}
				placeState = GridValue.DESTROYER;
			}
		});
		btnDestroyer.setBounds(32, 161, 145, 23);
		GamePiecePanel.add(btnDestroyer);
		shipButtons[4] = btnDestroyer;
		
		JLabel lblSize = new JLabel("Size: 5");
		lblSize.setFont(new Font("Tahoma", Font.BOLD, 11));
		lblSize.setBounds(203, 25, 41, 23);
		GamePiecePanel.add(lblSize);
		
		JLabel lblSize_1 = new JLabel("Size: 4");
		lblSize_1.setFont(new Font("Tahoma", Font.BOLD, 11));
		lblSize_1.setBounds(203, 59, 41, 23);
		GamePiecePanel.add(lblSize_1);
		
		JLabel lblSize_2 = new JLabel("Size: 3");
		lblSize_2.setFont(new Font("Tahoma", Font.BOLD, 11));
		lblSize_2.setBounds(203, 93, 41, 23);
		GamePiecePanel.add(lblSize_2);
		
		JLabel lblSize_3 = new JLabel("Size: 3");
		lblSize_3.setFont(new Font("Tahoma", Font.BOLD, 11));
		lblSize_3.setBounds(203, 127, 41, 23);
		GamePiecePanel.add(lblSize_3);
		
		JLabel lblSize_4 = new JLabel("Size: 2");
		lblSize_4.setFont(new Font("Tahoma", Font.BOLD, 11));
		lblSize_4.setBounds(203, 161, 41, 23);
		GamePiecePanel.add(lblSize_4);
		
		
		// ====================================================================================================================	
		// CHAT BOX
		// ====================================================================================================================	
		
		Label label = new Label("Chat");
		label.setFont(new Font("Dialog", Font.BOLD, 12));
		label.setBounds(293, 296, 37, 15);
		frame.getContentPane().add(label);
		
		JPanel ChatPanel = new JPanel();
		ChatPanel.setLayout(null);
		ChatPanel.setName("");
		ChatPanel.setBorder(new LineBorder(SystemColor.inactiveCaption, 1, true));
		ChatPanel.setBounds(285, 303, 389, 277);
		frame.getContentPane().add(ChatPanel);
		
		ChatArea = new JTextArea();
		ChatArea.setEditable(false);
		ChatArea.setLineWrap(true);
		ChatArea.setWrapStyleWord(true);
		
		JScrollPane sp = new JScrollPane(ChatArea);
		sp.setBounds(10, 11, 369, 224);
		ChatPanel.add(sp);
		
		ChatTextField = new JTextField();
		ChatTextField.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				sendChat();
			}
		});
		ChatTextField.setBounds(10, 241, 301, 27);
		ChatPanel.add(ChatTextField);
		ChatTextField.setColumns(10);
		
		Button SendButton = new Button("Send");
		SendButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				sendChat();
			}
		});
		SendButton.setBounds(317, 241, 62, 27);
		ChatPanel.add(SendButton);
		ChatPanel.setFocusTraversalPolicy(new FocusTraversalOnArray(new Component[]{sp, ChatArea, ChatTextField, SendButton}));
		
		// ====================================================================================================================	
		// create buttons and grid for opponent
		// ====================================================================================================================	
		
		OppGrid = new GridValue[10][10];
		OppButtons = new JButton[10][10];
		for (int i = 0; i < 10; i++){
			for (int j = 0; j < 10; j++){
				OppGrid[i][j] = GridValue.EMPTY;
				JButton b = new JButton("");
				OppButtons[i][j] = b;
				OppButtons[i][j].setEnabled(false);
				OppButtons[i][j].setBorder(new LineBorder(new Color(0, 0, 0)));
				//OppButtons[i][j].setBorder(new CompoundBorder(new LineBorder(new Color(0, 0, 0)), new LineBorder(new Color(255, 255, 255), 2)));
				OppButtons[i][j].setBackground(SystemColor.window);
				// create player mouse listener for different states
				
				OppButtons[i][j].addMouseListener(new java.awt.event.MouseAdapter(){
					public void mouseEntered(java.awt.event.MouseEvent evt){
						switch (gameState){
						case NOGAME:
							// DO NOTHING. PLAYER DOESN'T NEED TO DO 
							// ANYTHING WITH OPPONENTS GRID IN NOGAME.
							break;
						case SETUP:
							// DO NOTHING. PLAYER DOESN'T NEED TO DO 
							// ANYTHING WITH OPPONENTS GRID IN SETUP.
							break;
						case PLAY:
							b.setBackground(GridColor.SELECTED.c);
							// SEND MOUSE OVER VALUE TO OPPONENT FOR FEAR FACTOR.
							break;
						case GAMEOVER:
							break;
						}
					}
					public void mouseExited(java.awt.event.MouseEvent evt){
						switch (gameState){
						case NOGAME:
							// DO NOTHING. PLAYER DOESN'T NEED TO DO 
							// ANYTHING WITH OPPONENTS GRID IN SETUP.
							break;
						case SETUP:
							// DO NOTHING. PLAYER DOESN'T NEED TO DO 
							// ANYTHING WITH OPPONENTS GRID IN SETUP.
							break;
						case PLAY:
							// Set state back to normal
							b.setBackground(GridColor.EMPTY.c);
							break;
						case GAMEOVER:
							break;
						}
					}
					
					public void mouseClicked(java.awt.event.MouseEvent evt){
						switch (gameState){
						case NOGAME:
							// DO NOTHING. PLAYER DOESN'T NEED TO DO 
							// ANYTHING WITH OPPONENTS GRID IN SETUP.
							break;
						case SETUP:
							// DO NOTHING. PLAYER DOESN'T NEED TO DO 
							// ANYTHING WITH OPPONENTS GRID IN SETUP.
							break;
						case PLAY:
							b.setBackground(GridColor.HIT.c);
							// SEND MOUSE CLICK VALUE TO OPPONENT.
							break;
						case GAMEOVER:
							break;
						}
					}
					
				});
				OpponentGridPanel.add(OppButtons[i][j]);
			}
		}
		
		JMenuBar MenuBar = new JMenuBar();
		frame.setJMenuBar(MenuBar);
		
		JMenu mnMenu = new JMenu("Menu");
		MenuBar.add(mnMenu);
		
		mntmStartGame = new JMenuItem("Start Game");
		mntmStartGame.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				mntmStartGame.setEnabled(false);
				gameState = GameState.SETUP;
				startGame();
				GameStateText.setText("Choose ship below to place");
			}
		});
		mntmStartGame.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, InputEvent.CTRL_MASK));
		mnMenu.add(mntmStartGame);
		
		mntmQuitGame = new JMenuItem("Quit Game");
		mntmQuitGame.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				gameState = GameState.NOGAME;
				quitGame();
				mntmQuitGame.setEnabled(false);
			}
		});
		mntmQuitGame.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q, InputEvent.CTRL_MASK));
		mnMenu.add(mntmQuitGame);
		
		JMenuItem mntmAbout = new JMenuItem("About");
		mntmAbout.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_A, InputEvent.CTRL_MASK));
		mnMenu.add(mntmAbout);
		
		JMenuItem mntmExit = new JMenuItem("Exit");
		mntmExit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				System.exit(0);
			}
		});
		mntmExit.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_E, InputEvent.CTRL_MASK));
		mnMenu.add(mntmExit);
		frame.setFocusTraversalPolicy(new FocusTraversalOnArray(new Component[]{MenuBar, mnMenu, mntmStartGame, mntmQuitGame, PlayersLabel, mntmAbout, mntmExit, SendButton, ChatArea, ChatTextField, sp, ChatPanel, label, GamePiecePanel, GamePieceLabel, GameStateText, GameStatePanel, GameStateLabel, OpponentLabel, PlayersPanel, frame.getContentPane(), OpponentGridPanel, OpponentPanel, PlayerLabel, PlayerPanel, PlayerGridPanel}));
		
		draw();
	}
	
	private void draw() {
		for (int i = 0; i < 10; i++){
			for (int j = 0; j < 10; j++){
				// draw opponents grid
				switch(OppGrid[i][j]){
				case EMPTY: 
					OppButtons[i][j].setBackground(GridColor.EMPTY.c);
					break;
				case CARRIER:
					break;
				case BATTLESHIP:
					break;
				case CRUISER:
					break;
				case SUBMARINE:
					break;
				case DESTROYER: 
					break;
				case HIT:
					OppButtons[i][j].setBackground(GridColor.HIT.c);
					break;
				case SELECTED:
					OppButtons[i][j].setBackground(GridColor.SELECTED.c);
					break;
			}
				// draw players grid
				switch(PlayerGrid[i][j]){
				case EMPTY: 
					PlayerButtons[i][j].setBackground(GridColor.EMPTY.c);
					PlayerButtons[i][j].setBorder(new LineBorder(new Color(0, 0, 0)));
					break;
				case HIT:
					PlayerButtons[i][j].setBackground(GridColor.HIT.c);
					break;
				case SELECTED:
					PlayerButtons[i][j].setBackground(GridColor.SELECTED.c);
					break;
				default:
					PlayerButtons[i][j].setBackground(GridColor.SELECTED.c);
					PlayerButtons[i][j].setBorder(new CompoundBorder(new LineBorder(new Color(0, 0, 0)), new LineBorder(new Color(255, 255, 255), 2)));
					break;
			}
			}
		}
	}
	
	// ====================================================================================================================	
	// PLACE SHIP FUNCTION to change the state of the Player grid
	// ERROR if ship already exists at the location ship is to be place
	// ====================================================================================================================		
	
	private void sendChat(){
		if(ChatTextField.getText() != ""){
			ChatArea.setText(ChatArea.getText()+"You: "+ChatTextField.getText()+"\n");
			ChatTextField.setText("");
			// TODO: send text to server here
		}
	}
	
	// ====================================================================================================================	
	// PLACE SHIP FUNCTION to change the state of the Player grid
	// ERROR if ship already exists at the location ship is to be place
	// ====================================================================================================================	
	
	private void shipSet(JButton btn, int size){
		int i,j,x,offset = 0;
		draw();
		for(i = 0; i < 10; i++){
			for(j = 0; j < 10; j++){
				if (btn == PlayerButtons[i][j]){
					switch(placeOrient){
					// Horizontal placement
					case HORIZONTAL:
						if (10 - j < size){
							offset = j-(10-size);
						}
						for(x = 0; x < size; x++){
							if (PlayerGrid[i][j+x-offset] != GridValue.EMPTY){
								GameStateText.setText("Cannot place ship there");
								return;
							}
						}
						for(x = 0; x < size; x++){
							PlayerGrid[i][j+x-offset] = placeState;
						}
						break;
					// Vertical placement
					case VERTICAL:
						if (10 - i < size){
							offset = i-(10-size);
						}
						for(x = 0; x < size; x++){
							if (PlayerGrid[i+x-offset][j] != GridValue.EMPTY){
								GameStateText.setText("Cannot place ship there");
								return;
							}
						}
						for(x = 0; x < size; x++){
							PlayerGrid[i+x-offset][j] = placeState;
						}
						break;
					default:
						break;
					}
					shipButtons[placeState.num].setEnabled(false);
					placeState = GridValue.EMPTY;
					checkState();
					draw();
					return;
				}
			}
		}
		
	}
	
	// ====================================================================================================================	
	// HOVER SELECT SHIP FUNCTION to change the state of the Player grid
	// DISPLAY RED if ship already exists at the location ship is to be place
	// ====================================================================================================================	
	
	private void shipOver(JButton btn, int size){
		int i,j,x,offset = 0;
		boolean selected = false;
		draw();
		for(i = 0; i < 10; i++){
			for(j = 0; j < 10; j++){
				if (btn == PlayerButtons[i][j]){
					switch(placeOrient){
					// Horizontal placement
					case HORIZONTAL:
						if (10 - j < size){
							offset = j-(10-size);
						}
						for(x = 0; x < size; x++){
							if (PlayerGrid[i][j+x-offset] != GridValue.EMPTY){
								PlayerButtons[i][j+x-offset].setBackground(GridColor.HIT.c);
								selected = true;
							}
							else
								PlayerButtons[i][j+x-offset].setBackground(GridColor.SELECTED.c);
						}
						if(selected)
							return;
						for(x = 0; x < size; x++){
							PlayerButtons[i][j+x-offset].setBackground(GridColor.SELECTED.c);
						}
						break;
					// Vertical placement
					case VERTICAL:
						if (10 - i < size){
							offset = i-(10-size);
						}
						for(x = 0; x < size; x++){
							if (PlayerGrid[i+x-offset][j] != GridValue.EMPTY){
								PlayerButtons[i+x-offset][j].setBackground(GridColor.HIT.c);
								selected = true;
							}
							else
								PlayerButtons[i+x-offset][j].setBackground(GridColor.SELECTED.c);
						}
						if(selected){
							return;
						}
						for(x = 0; x < size; x++){
							PlayerButtons[i+x-offset][j].setBackground(GridColor.SELECTED.c);
						}
						break;
					default:
						break;
					}
					return;
				}
			}
		}
		
	}
	
	public void checkState(){
		int i,j,count = 0;
		for(i = 0; i < 10; i++){
			for(j = 0; j < 10; j++){
				if(PlayerGrid[i][j] != GridValue.EMPTY ){
					count++;
				}
			}
		}
		if (count == 17){
			gameState = GameState.PLAY;
			GameStateText.setText("You've placed all your ships!");
		}
	}
	
	public void startGame(){
		for (int i = 0; i < 5; i++){
			shipButtons[i].setEnabled(true);
		}
			
	}
	
	public void quitGame(){
		for (int i = 0; i < 5; i++){
			shipButtons[i].setEnabled(false);
		}
		int i,j,count = 0;
		for(i = 0; i < 10; i++){
			for(j = 0; j < 10; j++){
				PlayerGrid[i][j] = GridValue.EMPTY;
			}
		}
		mntmStartGame.setEnabled(true);
		draw();
			
	}

}