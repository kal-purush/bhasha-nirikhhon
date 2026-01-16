package Classes;

/**
 * Created by ignacioojanguren on 7/11/16.
 */
public class GoalsPlayer {
    
    private Player player;
    private int goals;
    private int timePlayed;
    private String avgGoalGame;

    public GoalsPlayer(Player player, int goals, int timePlayed, String avgGoalGame){
        this.player = player;
        this.goals = goals;
        this.timePlayed = timePlayed;
        this.avgGoalGame = avgGoalGame;
    }

    public void setPlayer (Player player){this.player = player;}
    public void setGoals (int goals){this.goals = goals;}
    public void setTimePlayed (int timePlayed){this.timePlayed = timePlayed;}
    public void setAvgGoalGame (String avgGoalGame){this.avgGoalGame = avgGoalGame;}

    public int getGoals() {return goals;}
    public int getTimePlayed() {return timePlayed;}
    public Player getPlayer() {return player;}
    public String getAvgGoalGame() {return avgGoalGame;}

    public double calculateProbability(Match nextMatch){
        /**
         * Calculate the probability a player has to score a goal given the conditions of the position of their teams,
         * the victories of their teams and where is the next match going to be played, if it is home or not.
         */
        return 0.0;
    }
}