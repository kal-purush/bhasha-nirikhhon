package rebuild;

import java.util.ArrayList;
import java.util.Objects;

import javax.swing.JFrame;

public class GameInstance {
	
	ArrayList<Player> players = new ArrayList<Player>();
	GameMaster gm = new GameMaster(20,20,4,2);
	
    public void startGame() {
    	gm.setBoard(gm.getBoardDim(0),gm.getBoardDim(1),gm.getDiff());
    }

 // Make separate class with it's own responsibility
    
    public void spawnRobots() {
    	ArrayList<int[]> corners = new ArrayList<int[]>();
    	corners.add(new int[] {0,0});
    	corners.add(new int[] {gm.getBoardDim(0)-1,gm.getBoardDim(1)-1});
    	corners.add(new int[] {0,gm.getBoardDim(1)-1});
    	corners.add(new int[] {gm.getBoardDim(0)-1,0});

    	ArrayList<Integer> directions = new ArrayList<Integer>();
    	directions.add(1); directions.add(2); directions.add(3); directions.add(0);

    	for (Robot robot : getRobots()) {
    		robot.setCoordinate(corners.get(0));
    		corners.remove(0);
    		robot.setInCorner(true);
    		robot.setDir(directions.get(0));
    		directions.remove(0);
    	}

    	for (int i = 0; i < gm.getPlayers().size(); i++) {
    		gm.getBoard().putRobotOn(getRobots().get(i).getCoordinate());
    		gm.getBoard().getTile(getRobots().get(i).getCoordinate()).placeRobot(true);
    		gm.getBoard().getTile(getRobots().get(i).getCoordinate()).repaint();
    	}
    }

 // Make separate class with it's own responsibility

    public void makePlayers() {
    	gm.setPlayers(players);
    	
    	for (int i = 0; i < gm.getPlayerAmount(); i++) {
    		players.add(new Player());
    	}

    	for(Player player : players) {
    		player.setBoardDim(gm.getBoardDim());
    		player.setBoard(gm.getBoard());
    	}
    }

// Make separate class with it's own responsibility

    public void startTurn() {
    	for(Player player : players) {
    		player.makeActionCards();
    		player.setMoved(false);
    		player.setStopTurn(false);
    	}
    }

// Make separate class with it's own responsibility

    public void execMoves() {
    	ArrayList<Player> playersSurvived = new ArrayList<Player>();
	
    	for(int i = 0; i < 4; i++) {
    		for(Player player : players) {
    			player.setBoard(gm.getBoard());
    			// setBoard(player.execHand(i)); not sure how to refactor this bit,is player.exechand(i) a board?
    		}	   
    	}
	
    	for(Player player : players) {
    		if(!player.getRobot().isDead()) {
    			playersSurvived.add(player);
    		}
    	}
	
    	players = playersSurvived;
    }
    
    public void play(JFrame f) {
    	f.add(gm.getBoard());
		f.setSize(700,700);
		f.setVisible(true);
		f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }
        
    public ArrayList<Robot> getRobots() {
    	ArrayList<Robot> robots = new ArrayList<Robot>();

    	for (int i = 0; i < players.size(); i++) {
    		robots.add(players.get(i).getRobot());
    	}
    	return robots;
    }

    public ArrayList<Player> getPlayers() {
    	return players;
    }
}