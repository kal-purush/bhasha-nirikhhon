import java.awt.Color;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.swing.JOptionPane;

public class Battleship {
    public static Network gameNetwork = null;
    public static Frame gameFrame = null;
    public static int serVal = 0;
    
    public static void main (String [] args){
        gameFrame = new Frame();
        //The following creates the network and establishes the role of the player.
        serVal = JOptionPane.showConfirmDialog(null, "Are you the server? (Y/N)", "Server?", JOptionPane.YES_NO_OPTION);
        if(serVal == JOptionPane.YES_OPTION){
        	try {
        		gameNetwork = new Network(true, true);
			} catch (IOException e) {
				gameFrame.printConsole("NETWORK Error\n");
				e.printStackTrace();
			}
        }
        else{
        	try {
        		gameNetwork = new Network(false, true);
			} catch (IOException e) {
				gameFrame.printConsole("NETWORK Error\n");
				e.printStackTrace();
			}
        }
        gameFrame.init();
        gameFrame.printConsole("Welcome to Battleship v2.2.\n");
        gameFrame.printConsole("Created by Team Aqua.\n");
        if(serVal == JOptionPane.YES_OPTION){									//Helps identify who is who in the game.
        	gameFrame.printConsole("Ready, Player One?\n");						//Server
        }
        else{
        	gameFrame.printConsole("Ready, Player Two?\n");						//Client
        }
        
        //This prints the rules for the player if they don't know them.
        int ruleVal = JOptionPane.showConfirmDialog(null, "Do you know the rules? (Y/N)", "Rules?", JOptionPane.YES_NO_OPTION);
        if(ruleVal == JOptionPane.NO_OPTION){
        	printRules();
        }
        boolean placed = gameFrame.ownShips.isPlaced();
        while(!placed){
        	try {
        		TimeUnit.MILLISECONDS.sleep(250);
	        	placed = gameFrame.ownShips.isPlaced();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
        gameNetwork.donePlacing = true;
        try {
			gameNetwork.placeBoats();
		} catch (IOException e1) {
			System.out.println("HOLY TOLEDO.");
			e1.printStackTrace();
		}
        gameFrame.start();
        int turnVal = 0;
        if (serVal == 0 && gameNetwork.getMagicNumber() == 0){
        	turnVal = 0;
        }
        else if (serVal == 1){
        	turnVal = gameNetwork.getMagicNumber();
        }
        
        int myShipCount = 5;
        int enemyShipCount = 5;
        while (isAlive(gameNetwork.getServerBool()) == true){
        	if (turnVal == 0){
        		gameFrame.printConsole("Choose a square to launch a missle at.\n");
            	gameFrame.printConsole("Click fire when you are ready.\n");
            	String atkArg = "";
            	gameFrame.shotsFired = false;
            	while(!(gameFrame.shotsFired)){
            		try {
						TimeUnit.MILLISECONDS.sleep(250);
					} catch (InterruptedException e) {
						System.out.println("No Shots Fired Today.");
						e.printStackTrace();
					}
            	}
            	atkArg = gameFrame.fire();
            	gameFrame.shotsFired = false;
            	String response = "";
				try {
					response = gameNetwork.attack(atkArg);
				} catch (IOException e) {
					System.out.println("Attack/Response Error.");
					e.printStackTrace();
				}
            	
            	//parse response x,y,B:SID
            	String[] reply = response.split(":");
            	String[] shotInfo = reply[0].split(",");
            	int x = Integer.parseInt(shotInfo[0]);
            	int y = Integer.parseInt(shotInfo[1]);
            	int B = Integer.parseInt(shotInfo[2]);
            	int SID = Integer.parseInt(reply[1]);
            	
            	if (SID == 0 && B == 1){
            		gameFrame.btnGrid.selectHit(x,y);							//Set (x,y)  enemy square to hit
            	}
            	else if (B == 0){
            		gameFrame.btnGrid.selectMiss(x,y);							//Set (x,y) enemy square to miss
            	}
            	else if (SID > 0 && B == 1){
            		gameFrame.printConsole("You sunk my " + getShipName(SID) + "!\n");
            		gameFrame.btnGrid.selectHit(x,y);							//Set (x,y) enemy square to miss
            		enemyShipCount--;
            	}
            	turnVal = 1;
            	gameFrame.setTextColor(Color.GREEN);
        	}
        	else if (turnVal == 1){
        		gameFrame.printConsole("The other player is firing.\n");
            	gameFrame.printConsole("Please Wait...\n");
            	String defArg = null;
            	while (defArg == null){
            		try {
            			TimeUnit.MILLISECONDS.sleep(250);
            			defArg = gameNetwork.defend();
            		} catch (InterruptedException | IOException e) {
            			System.out.println("Defending Error.");
            			e.printStackTrace();
            		}
				}
            	//parses attack ATK:x,y
            	String[] attack = defArg.split(":");			
            	String[] shotInfo = attack[1].split(",");			
            	//String attackCode = attack[0];           				  Not used for special shots
            	int x = Integer.parseInt(shotInfo[0]);
            	int y = Integer.parseInt(shotInfo[1]);
            	int[] hitMiss = gameFrame.ownShips.attacked(x, y);
            	int B = hitMiss[0];
            	if(B == 1){
            		gameFrame.printConsole("Your ship on " + "(" + x + "," + y + ") " + "was hit!\n");
            	}
            	else{
            		gameFrame.printConsole("Your opponent missed your ships, firing at " + "(" + x + "," + y + ").\n");
            	}
            	int SID = hitMiss[1];
            	if (SID > 0){
            		myShipCount--;
            	}
            	//Creates response x,y,B:SID
            	String repArg = "" + x + "," + y + "," + B + ":" + SID;
            	try {
					gameNetwork.defendReply(repArg);
				} catch (IOException e) {
					e.printStackTrace();
				}
            	turnVal = 0;
        	}
        	if (myShipCount == 0){
        		gameFrame.printConsole("GAME OVER.\nYou win!!!! :)");
        		break;
        	} else if (enemyShipCount == 0){
        		gameFrame.printConsole("GAME OVER.\nYou lost!!! :(");
        		break;
        	}
        }
    }
    
    private static boolean isAlive(boolean serverOrClient){
    	boolean schroodinger = false;
    	if(serverOrClient){
    		schroodinger = Network.serverSocketC.isConnected();
    	}
    	else{
    		schroodinger = Network.clientSocket.isConnected();
    	}
    	return schroodinger;
    }
    
    public static String getShipName(int ID){
    	String name = "";
    	switch (ID){
    	case 1:
    		name = "Patrol Boat";
    		break;
    	case 2:
    		name = "Submarine";
    		break;
    	case 3:
    		name = "Destroyer";
    		break;
    	case 4:
    		name = "Battleship";
    		break;
    	case 5:
    		name = "Aircraft Carrier";
    		break;
    	default:
    		name = "MissingNo.";
    		break;
    	}
    	return name;
    }

	public static void printRules(){
		gameFrame.printConsole("*****Rules for BattleShip*****\n" +
    			"\nGame Objective\n" +
    			"The object of Battleship is to try and sink all of the other player's before they sink all of your ships. All of the other player's ships are somewhere on his/her board.\n" +
    			"You try and hit them by calling out the coordinates of one of the squares on the board.\n" + 
    			"The other player also tries to hit your ships by calling out coordinates.\n" +
    			"Neither you nor the other player can see the other's board so you must try to guess where they are.\n" +
    			"Each board in the physical game has two grids:  the lower (horizontal) section for the player's ships and the upper part (vertical during play) for recording the player's guesses.\n" +
    			"\nStarting a New Game\n"+
    			"Each player places the 5 ships somewhere on their board.\n" +
    			"The ships can only be placed vertically or horizontally. Diagonal placement is not allowed.\n" +
    			"No part of a ship may hang off the edge of the board.  Ships may not overlap each other.\n" +
    			"No ships may be placed on another ship. Once the guessing begins, the players may not move the ships.\n" +
    			"The 5 ships are:  Carrier (occupies 5 spaces), Battleship (4), Cruiser (3), Submarine (3), and Destroyer (2).\n" +
    			"\nPlaying the Game\n" +
    			"Player's take turns guessing by calling out the coordinates. The opponent responds with \"hit\" or \"miss\" as appropriate.\n" +
    			"Both players should mark their board with pegs:  red for hit, white for miss.\n" +
    			"For example, if you call out F6 and your opponent does not have any ship located at F6, your opponent would respond with \"miss\".\n" +
    			"You record the miss F6 by placing a white peg on the lower part of your board at F6. Your opponent records the miss by placing.\n" +
    			"When all of the squares that one your ships occupies have been hit, the ship will be sunk.\n" +
    			"You should announce \"hit and sunk\".\n" +
    			"In the physical game, a red peg is placed on the top edge of the vertical board to indicate a sunk ship.\n" +
    			"As soon as all of one player's ships have been sunk, the game ends.\n" +
    			"\n*****************************\n");
    }
}