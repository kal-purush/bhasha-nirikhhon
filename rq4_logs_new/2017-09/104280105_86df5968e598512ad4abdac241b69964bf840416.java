import javax.swing.*;
import java.awt.*;
import java.awt.event.*; 

public class ConnectFour extends JFrame implements ActionListener {
	//data constants
	public int[][] data = {
		{-1,-1,-1,-1,-1,-1,-1},
		{-1,-1,-1,-1,-1,-1,-1},
		{-1,-1,-1,-1,-1,-1,-1},
		{-1,-1,-1,-1,-1,-1,-1},
		{-1,-1,-1,-1,-1,-1,-1},
		{-1,-1,-1,-1,-1,-1,-1}
	};

	public int turn = 0;
	public boolean win = false;
	public boolean draw = false;
	public int column;
	
	//drawing constants
	private JPanel board;
	private JPanel pieces;
	private JButton newGame;
	private JButton turnDisplay;
	private JButton exit;
	private Container boardPane;
	private Container piecePane;

	//Pieces
	private JButton b00; private JButton b10; private JButton b20;
	private JButton b01; private JButton b11; private JButton b21;
	private JButton b02; private JButton b12; private JButton b22;
	private JButton b03; private JButton b13; private JButton b23;
	private JButton b04; private JButton b14; private JButton b24;
	private JButton b05; private JButton b15; private JButton b25;
	private JButton b06; private JButton b16; private JButton b26;
	
	private JButton b30; private JButton b40; private JButton b50;
	private JButton b31; private JButton b41; private JButton b51;
	private JButton b32; private JButton b42; private JButton b52;
	private JButton b33; private JButton b43; private JButton b53;
	private JButton b34; private JButton b44; private JButton b54;
	private JButton b35; private JButton b45; private JButton b55;
	private JButton b36; private JButton b46; private JButton b56;
	
	//init properties
	public ConnectFour() {
		super("Connect Four");
    	setSize(1000,1000);
    	setResizable(false);
    	setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    	
		
		//init panels
		board = new JPanel();
		board.setLayout(new GridLayout(1,3));
		pieces = new JPanel();
		pieces.setLayout(new GridLayout(6,7));

		//init buttons
		newGame = new JButton("New Game");
		newGame.addActionListener(this);
		board.add(newGame);
		turnDisplay = new JButton("Turn");
		turnDisplay.addActionListener(this);
		board.add(turnDisplay);
		turnDisplay.setForeground(Color.white);
		if(turn == 1) {turnDisplay.setBackground( Color.red );}
		else {turnDisplay.setBackground( Color.black );}
		exit = new JButton("Exit");
		exit.addActionListener(this);
		board.add(exit);

		//init pieces
		b00 = new JButton(); b00.addActionListener(this); pieces.add(b00);
		b01 = new JButton(); b01.addActionListener(this); pieces.add(b01);
		b02 = new JButton(); b02.addActionListener(this); pieces.add(b02);
		b03 = new JButton(); b03.addActionListener(this); pieces.add(b03);
		b04 = new JButton(); b04.addActionListener(this); pieces.add(b04);
		b05 = new JButton(); b05.addActionListener(this); pieces.add(b05);
		b06 = new JButton(); b06.addActionListener(this); pieces.add(b06);

		b10 = new JButton(); b10.addActionListener(this); pieces.add(b10);
		b11 = new JButton(); b11.addActionListener(this); pieces.add(b11);
		b12 = new JButton(); b12.addActionListener(this); pieces.add(b12);
		b13 = new JButton(); b13.addActionListener(this); pieces.add(b13);
		b14 = new JButton(); b14.addActionListener(this); pieces.add(b14);
		b15 = new JButton(); b15.addActionListener(this); pieces.add(b15);
		b16 = new JButton(); b16.addActionListener(this); pieces.add(b16);

		b20 = new JButton(); b20.addActionListener(this); pieces.add(b20);
		b21 = new JButton(); b21.addActionListener(this); pieces.add(b21);
		b22 = new JButton(); b22.addActionListener(this); pieces.add(b22);
		b23 = new JButton(); b23.addActionListener(this); pieces.add(b23);
		b24 = new JButton(); b24.addActionListener(this); pieces.add(b24);
		b25 = new JButton(); b25.addActionListener(this); pieces.add(b25);
		b26 = new JButton(); b26.addActionListener(this); pieces.add(b26);

		b30 = new JButton(); b30.addActionListener(this); pieces.add(b30);
		b31 = new JButton(); b31.addActionListener(this); pieces.add(b31);
		b32 = new JButton(); b32.addActionListener(this); pieces.add(b32);
		b33 = new JButton(); b33.addActionListener(this); pieces.add(b33);
		b34 = new JButton(); b34.addActionListener(this); pieces.add(b34);
		b35 = new JButton(); b35.addActionListener(this); pieces.add(b35);
		b36 = new JButton(); b36.addActionListener(this); pieces.add(b36);

		b40 = new JButton(); b40.addActionListener(this); pieces.add(b40);
		b41 = new JButton(); b41.addActionListener(this); pieces.add(b41);
		b42 = new JButton(); b42.addActionListener(this); pieces.add(b42);
		b43 = new JButton(); b43.addActionListener(this); pieces.add(b43);
		b44 = new JButton(); b44.addActionListener(this); pieces.add(b44);
		b45 = new JButton(); b45.addActionListener(this); pieces.add(b45);
		b46 = new JButton(); b46.addActionListener(this); pieces.add(b46);

		b50 = new JButton(); b50.addActionListener(this); pieces.add(b50);
		b51 = new JButton(); b51.addActionListener(this); pieces.add(b51);
		b52 = new JButton(); b52.addActionListener(this); pieces.add(b52);
		b53 = new JButton(); b53.addActionListener(this); pieces.add(b53);
		b54 = new JButton(); b54.addActionListener(this); pieces.add(b54);
		b55 = new JButton(); b55.addActionListener(this); pieces.add(b55);
		b56 = new JButton(); b56.addActionListener(this); pieces.add(b56);


		//add to Container
		boardPane = this.getContentPane();
		boardPane.add(board,BorderLayout.NORTH);
		piecePane = this.getContentPane();
		piecePane.add(pieces,BorderLayout.CENTER);
		clearButtons();


		

	}
	//self explanatory
	public void winCheck() {
		horzCheck();
		ldiagCheck();
		rdiagCheck();
		vertCheck();
		if(win == true) {
			if(turn == 1) {
				redWin();
			}
			else {
				blackWin();
			}

		}	
	}
	public void drawCheck() {
		int count = 0;
		for (int x = 0; x < data[0].length; x++ ) {
			if(data[0][x] != -1) {
				count++;
				if (count >= 7) {
					clearButtons();
				}
			}
			else {count = 0;}
		}
	}
	//Check patterns horzontally
	public void horzCheck() {

			int count = 0;
			int i = -1;
			for (int y=0; y<data.length; y++) {
			
				for (int x=0; x<data[0].length; x++) {
					i = data[y][x];
					if(i==turn) {
						count++;
						if(count>=4) { win = true; }
					}	
					else { count = 0; }
				}			
			}
	}
	//Check for forward slash pattens
	public void ldiagCheck() {

			for (int x=0; x<4; x++) {
				for (int y=0; y<3; y++) {
					if(data[y][x] == turn) {
						if(data[y+1][x+1] == turn) {
							if(data[y+2][x+2] == turn) {
								if(data[y+3][x+3] == turn) {
									win = true;
								}

							}
						}	
					}
			
				}
			
			}
	}
	//Check for backslash patterns
	public void rdiagCheck() {

			for (int x=6; x>2; x--) {
				for (int y=0; y<3; y++) {
					if(data[y][x] == turn) {
						if(data[y+1][x-1] == turn) {
							if(data[y+2][x-2] == turn) {
								if(data[y+3][x-3] == turn) {
									win = true;
								}

							}
						}	
					}
			
				}
			
			}
	}
	//Check patterns verticlly
	public void vertCheck() {
			int count = 0;
				int i = -1;
				for (int x=0; x<data[0].length; x++) {

					for (int y=0; y<data.length; y++) {

						i = data[y][x];
						if(i==turn) {
							count++;
							if(count>=4) { win = true; }
						}
						else { count = 0; }

					}

				}
	}
	//Simulates gravity moving the piece to the lowest slot
	public void gravity(int row) {
		for (column=5;column>=0;column--) {
			if( data[column][row] == -1) {
				data[column][row] = turn;
				break;
			}	
		}
	}
	//turns the data array into a GUI
	public void paintButtons(int row) {
		if(turn == 1) {
			switch(row) {
				case 0:switch(column) {
							case 0:b00.setBackground( Color.red);break;
							case 1:b10.setBackground( Color.red);break;
							case 2:b20.setBackground( Color.red);break;
							case 3:b30.setBackground( Color.red);break;
							case 4:b40.setBackground( Color.red);break;
							case 5:b50.setBackground( Color.red);break;
				} break;
				case 1:switch(column) {
							case 0:b01.setBackground( Color.red);break;
							case 1:b11.setBackground( Color.red);break;
							case 2:b21.setBackground( Color.red);break;
							case 3:b31.setBackground( Color.red);break;
							case 4:b41.setBackground( Color.red);break;
							case 5:b51.setBackground( Color.red);break;
				} break;
				case 2:switch(column) {
							case 0:b02.setBackground( Color.red);break;
							case 1:b12.setBackground( Color.red);break;
							case 2:b22.setBackground( Color.red);break;
							case 3:b32.setBackground( Color.red);break;
							case 4:b42.setBackground( Color.red);break;
							case 5:b52.setBackground( Color.red);break;
				} break;
				case 3:switch(column) {
							case 0:b03.setBackground( Color.red);break;
							case 1:b13.setBackground( Color.red);break;
							case 2:b23.setBackground( Color.red);break;
							case 3:b33.setBackground( Color.red);break;
							case 4:b43.setBackground( Color.red);break;
							case 5:b53.setBackground( Color.red);break;
				} break;
				case 4:switch(column) {
							case 0:b04.setBackground( Color.red);break;
							case 1:b14.setBackground( Color.red);break;
							case 2:b24.setBackground( Color.red);break;
							case 3:b34.setBackground( Color.red);break;
							case 4:b44.setBackground( Color.red);break;
							case 5:b54.setBackground( Color.red);break;
				} break;
				case 5:switch(column) {
							case 0:b05.setBackground( Color.red);break;
							case 1:b15.setBackground( Color.red);break;
							case 2:b25.setBackground( Color.red);break;
							case 3:b35.setBackground( Color.red);break;
							case 4:b45.setBackground( Color.red);break;
							case 5:b55.setBackground( Color.red);break;
				} break;
				case 6:switch(column) {
							case 0:b06.setBackground( Color.red);break;
							case 1:b16.setBackground( Color.red);break;
							case 2:b26.setBackground( Color.red);break;
							case 3:b36.setBackground( Color.red);break;
							case 4:b46.setBackground( Color.red);break;
							case 5:b56.setBackground( Color.red);break;
				} break;
			}
		}
		else {
			switch(row) {
				case 0:switch(column) {
							case 0:b00.setBackground( Color.black);break;
							case 1:b10.setBackground( Color.black);break;
							case 2:b20.setBackground( Color.black);break;
							case 3:b30.setBackground( Color.black);break;
							case 4:b40.setBackground( Color.black);break;
							case 5:b50.setBackground( Color.black);break;
				} break;
				case 1:switch(column) {
							case 0:b01.setBackground( Color.black);break;
							case 1:b11.setBackground( Color.black);break;
							case 2:b21.setBackground( Color.black);break;
							case 3:b31.setBackground( Color.black);break;
							case 4:b41.setBackground( Color.black);break;
							case 5:b51.setBackground( Color.black);break;
				} break;
				case 2:switch(column) {
							case 0:b02.setBackground( Color.black);break;
							case 1:b12.setBackground( Color.black);break;
							case 2:b22.setBackground( Color.black);break;
							case 3:b32.setBackground( Color.black);break;
							case 4:b42.setBackground( Color.black);break;
							case 5:b52.setBackground( Color.black);break;
				} break;
				case 3:switch(column) {
							case 0:b03.setBackground( Color.black);break;
							case 1:b13.setBackground( Color.black);break;
							case 2:b23.setBackground( Color.black);break;
							case 3:b33.setBackground( Color.black);break;
							case 4:b43.setBackground( Color.black);break;
							case 5:b53.setBackground( Color.black);break;
				} break;
				case 4:switch(column) {
							case 0:b04.setBackground( Color.black);break;
							case 1:b14.setBackground( Color.black);break;
							case 2:b24.setBackground( Color.black);break;
							case 3:b34.setBackground( Color.black);break;
							case 4:b44.setBackground( Color.black);break;
							case 5:b54.setBackground( Color.black);break;
				} break;
				case 5:switch(column) {
							case 0:b05.setBackground( Color.black);break;
							case 1:b15.setBackground( Color.black);break;
							case 2:b25.setBackground( Color.black);break;
							case 3:b35.setBackground( Color.black);break;
							case 4:b45.setBackground( Color.black);break;
							case 5:b55.setBackground( Color.black);break;
				} break;
				case 6:switch(column) {
							case 0:b06.setBackground( Color.black);break;
							case 1:b16.setBackground( Color.black);break;
							case 2:b26.setBackground( Color.black);break;
							case 3:b36.setBackground( Color.black);break;
							case 4:b46.setBackground( Color.black);break;
							case 5:b56.setBackground( Color.black);break;
				} break;
			}	
		}
	}
	//red out the board when red wins
	public void redWin() {
	
		b00.setBackground( Color.red ); b30.setBackground( Color.red );
		b01.setBackground( Color.red ); b31.setBackground( Color.red );
		b02.setBackground( Color.red ); b32.setBackground( Color.red );
		b03.setBackground( Color.red ); b33.setBackground( Color.red );
		b04.setBackground( Color.red ); b34.setBackground( Color.red );
		b05.setBackground( Color.red ); b35.setBackground( Color.red );
		b06.setBackground( Color.red ); b36.setBackground( Color.red );

		b10.setBackground( Color.red ); b40.setBackground( Color.red );
		b11.setBackground( Color.red ); b41.setBackground( Color.red );
		b12.setBackground( Color.red ); b42.setBackground( Color.red );
		b13.setBackground( Color.red ); b43.setBackground( Color.red );
		b14.setBackground( Color.red ); b44.setBackground( Color.red );
		b15.setBackground( Color.red ); b45.setBackground( Color.red );
		b16.setBackground( Color.red ); b46.setBackground( Color.red );

		b20.setBackground( Color.red ); b50.setBackground( Color.red );
		b21.setBackground( Color.red ); b51.setBackground( Color.red );
		b22.setBackground( Color.red ); b52.setBackground( Color.red );
		b23.setBackground( Color.red ); b53.setBackground( Color.red );
		b24.setBackground( Color.red ); b54.setBackground( Color.red );
		b25.setBackground( Color.red ); b55.setBackground( Color.red );
		b26.setBackground( Color.red ); b56.setBackground( Color.red );
	}
	//blackout the board when black wins
	public void blackWin() {
	
		b00.setBackground( Color.black ); b30.setBackground( Color.black );
		b01.setBackground( Color.black ); b31.setBackground( Color.black );
		b02.setBackground( Color.black ); b32.setBackground( Color.black );
		b03.setBackground( Color.black ); b33.setBackground( Color.black );
		b04.setBackground( Color.black ); b34.setBackground( Color.black );
		b05.setBackground( Color.black ); b35.setBackground( Color.black );
		b06.setBackground( Color.black ); b36.setBackground( Color.black );

		b10.setBackground( Color.black ); b40.setBackground( Color.black );
		b11.setBackground( Color.black ); b41.setBackground( Color.black );
		b12.setBackground( Color.black ); b42.setBackground( Color.black );
		b13.setBackground( Color.black ); b43.setBackground( Color.black );
		b14.setBackground( Color.black ); b44.setBackground( Color.black );
		b15.setBackground( Color.black ); b45.setBackground( Color.black );
		b16.setBackground( Color.black ); b46.setBackground( Color.black );

		b20.setBackground( Color.black ); b50.setBackground( Color.black );
		b21.setBackground( Color.black ); b51.setBackground( Color.black );
		b22.setBackground( Color.black ); b52.setBackground( Color.black );
		b23.setBackground( Color.black ); b53.setBackground( Color.black );
		b24.setBackground( Color.black ); b54.setBackground( Color.black );
		b25.setBackground( Color.black ); b55.setBackground( Color.black );
		b26.setBackground( Color.black ); b56.setBackground( Color.black );
	}
	//new game init
	public void clearButtons() {
		win = false;
		for(int i = 0; i < data.length; i++) {
			for (int j = 0; j <data[0].length;j++) {
				data[i][j] = -1; 
			}
		}
		b00.setBackground( Color.white ); b30.setBackground( Color.white );
		b01.setBackground( Color.white ); b31.setBackground( Color.white );
		b02.setBackground( Color.white ); b32.setBackground( Color.white );
		b03.setBackground( Color.white ); b33.setBackground( Color.white );
		b04.setBackground( Color.white ); b34.setBackground( Color.white );
		b05.setBackground( Color.white ); b35.setBackground( Color.white );
		b06.setBackground( Color.white ); b36.setBackground( Color.white );

		b10.setBackground( Color.white ); b40.setBackground( Color.white );
		b11.setBackground( Color.white ); b41.setBackground( Color.white );
		b12.setBackground( Color.white ); b42.setBackground( Color.white );
		b13.setBackground( Color.white ); b43.setBackground( Color.white );
		b14.setBackground( Color.white ); b44.setBackground( Color.white );
		b15.setBackground( Color.white ); b45.setBackground( Color.white );
		b16.setBackground( Color.white ); b46.setBackground( Color.white );

		b20.setBackground( Color.white ); b50.setBackground( Color.white );
		b21.setBackground( Color.white ); b51.setBackground( Color.white );
		b22.setBackground( Color.white ); b52.setBackground( Color.white );
		b23.setBackground( Color.white ); b53.setBackground( Color.white );
		b24.setBackground( Color.white ); b54.setBackground( Color.white );
		b25.setBackground( Color.white ); b55.setBackground( Color.white );
		b26.setBackground( Color.white ); b56.setBackground( Color.white );
	}
	// event handler
	public void actionPerformed(ActionEvent event)
	{
		Object source = event.getSource();
		if (source == newGame) { 

			clearButtons();
		}

		else if (source == exit) {	
			System.exit(0);
		}
		else if(source==b00||source==b10||source==b20||
				source==b30||source==b40||source==b50) {
			if(data[0][0]!=0 && data[0][0]!=1) {
				gravity(0);
				paintButtons(0);
				winCheck();
				drawCheck();
				if (turn == 0) {turn = 1;}
				else {turn = 0;}
				if(turn == 1) {turnDisplay.setBackground( Color.red );}
				else {turnDisplay.setBackground( Color.black );}

			}		
		}
		else if(source==b01||source==b11||source==b21||
				source==b31||source==b41||source==b51) {
			if(data[0][1]!=0 && data[0][1]!=1) {
				gravity(1);	
				paintButtons(1);
				winCheck();
				drawCheck();
				if (turn == 0) {turn = 1;}
				else {turn = 0;}
				if(turn == 1) {turnDisplay.setBackground( Color.red );}
				else {turnDisplay.setBackground( Color.black );}
			}	
		}
		else if(source==b02||source==b12||source==b22||
				source==b32||source==b42||source==b52) {
			if(data[0][2]!=0 && data[0][2]!=1) {
				gravity(2);	
				paintButtons(2);
				winCheck();
				drawCheck();
				if (turn == 0) {turn = 1;}
				else {turn = 0;}
				if(turn == 1) {turnDisplay.setBackground( Color.red );}
				else {turnDisplay.setBackground( Color.black );}
			}	
		}
		else if(source==b03||source==b13||source==b23||
				source==b33||source==b43||source==b53) {
			if(data[0][3]!=0 && data[0][3]!=1) {
				gravity(3);	
				paintButtons(3);
				winCheck();
				drawCheck();
				if (turn == 0) {turn = 1;}
				else {turn = 0;}
				if(turn == 1) {turnDisplay.setBackground( Color.red );}
				else {turnDisplay.setBackground( Color.black );}
			}
		}
		else if(source==b04||source==b14||source==b24||
				source==b34||source==b44||source==b54) {
			if(data[0][4]!=0 && data[0][4]!=1) {
				gravity(4);
				paintButtons(4);
				winCheck();
				drawCheck();
				if (turn == 0) {turn = 1;}
				else {turn = 0;}
				if(turn == 1) {turnDisplay.setBackground( Color.red );}
				else {turnDisplay.setBackground( Color.black );}
			}		
		}
		else if(source==b05||source==b15||source==b25||
				source==b35||source==b45||source==b55) {
			if(data[0][5]!=0 && data[0][5]!=1) {
				gravity(5);	
				paintButtons(5);
				winCheck();
				drawCheck();
				if (turn == 0) {turn = 1;}
				else {turn = 0;}
				if(turn == 1) {turnDisplay.setBackground( Color.red );}
				else {turnDisplay.setBackground( Color.black );}
			}
		}
		else if(source==b06||source==b16||source==b26||
					source==b36||source==b46||source==b56) {
			if(data[0][6]!=0 && data[0][6]!=1) {
				gravity(6);	
				paintButtons(6);
				winCheck();
				drawCheck();
				if (turn == 0) {turn = 1;}
				else {turn = 0;}
				if(turn == 1) {turnDisplay.setBackground( Color.red );}
				else {turnDisplay.setBackground( Color.black );}
				}
			}
		
	}
	//instantiates the GUI Object
	public static void main(String[] args) {
		ConnectFour window = new ConnectFour();
		window.setVisible(true);
	}
}