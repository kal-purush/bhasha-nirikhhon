package Calculation;

import Classes.GoalsPlayer;
import Classes.Player;
import Classes.Team;

/**
 * Created by ignacioojanguren on 8/11/16.
 */
public class Probabilities {

    public static double calculatePlayerGoalsInTeam(GoalsPlayer goalsPlayer, Team team){
        double probability = 0;
        int minutesPlayed = goalsPlayer.getTimePlayed(), numberGoals = goalsPlayer.getGoals(), goalsTeam = team.getGoalsFor();

        probability = (numberGoals * 100) / goalsTeam;

        return probability;
    }

    public static double calculatePlayerScoresToTeam(Player player, Team team, GoalsPlayer goalsPlayer){
        return 0.0;
    }

}