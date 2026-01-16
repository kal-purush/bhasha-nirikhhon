package skunk.domain;

public class Player {

	private int chipCount = 50;
	private int playerScore = 0;
	int playerNum = 0;
	String playerName = null;
	Game newGame = new Game();
	Roll roll = new Roll();
//	Skipping player takes a turn and going directly to Roll class using Roll and Skunk
//	"classes to control the turn and return the results of the turn".

	public Player(int playerCount, String playerID) {
		this.playerName = playerID;
		this.playerNum = playerCount;
	}

	public Player() {

	}

	public void setPlayer() {
		playerName = newGame.getPlayerName();
		playerNum = newGame.getNumPlayers();
	}

	public String getPlayerName() {
		return this.playerName;
	}

	public int getPlayerNum() {
		return this.playerNum;
	}

	public int getPlayerScore() {
		this.playerScore += newGame.getGameScore();
		return this.playerScore;
	}

	public int getPlayerChipCount() {
		if (roll.getDie1Score() == 1 & roll.getDie2Score() == 1) {
			this.chipCount = 0;
			return this.chipCount;
		} else {
			return this.chipCount += roll.updateChipCount();
		}
	}

	public void setPlayerChipCount() { // 12/14/19
		this.chipCount = 0;
	}

}