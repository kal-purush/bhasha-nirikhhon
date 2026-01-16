package Calculation;

import Classes.Match;
import java.util.ArrayList;

/**
 * Created by ignacioojanguren on 4/11/16.
 * This class allows to make calculations about the matches already played by all the teams.
 *
 * It has a few methods that allows to calculate the goals scored by each team
 *
 */
public class CalculateMatches {

    /*public static double percentageVictory(Team team1, Team team2){

    }*/

    /**
     * CalculateGoalsLocal allows us to calculate all the goals a team playing in his field has scored.
     * We suppose the team exists.
     * @param teamName
     *  Name of the team we want to know how many goals has scored playing in his field.
     * @param matches
     *  ArrayList with all the games played. To obtain that list we use the class ReadMatches.
     * @return
     *  returns an integer with the number of goals scored
     */
    public static int calculateGoalsLocal(String teamName,ArrayList<Match> matches){
        int totalGoals = 0;
        for(Match match : matches){
            if(match.getLocalTeam().getName().equals(teamName)){
                totalGoals += match.getGoalLocal();
            }
        }
        return totalGoals;
    }

    /**
     * CalculateGoalsVisitant allows us to calculate all the goals a team has scored in another field.
     * We suppose the name of the team exists
     * @param teamName
     *  Name of the team we want to know how many goals has scored
     * @param matches
     *  ArrayList with all the games played. To obtain that list we use the class ReadMatches.
     * @return
     *  Integer with the numbers of goals scored
     */
    public static int calculateGoalsVisitant(String teamName,ArrayList<Match> matches){
        int totalGoals = 0;
        for(Match match : matches){
            if(match.getVisitantTeam().getName().equals(teamName)){
                totalGoals += match.getGoalVisitant();
            }
        }
        return totalGoals;
    }

    /**
     * CalculateTotalGoals calculate the total of goals a team has scores at any game.
     * @param teamName
     *  Name of the team we want to know how many goals has scored.
     * @param matches
     *  ArrayList with all the games played. To obtain that list we use the class ReadMatches.
     * @return
     *  Number of goals a team has scored in the season 2016-2017
     */
    public static int calculateTotalGoals(String teamName, ArrayList<Match> matches){
        return calculateGoalsLocal(teamName, matches) + calculateGoalsVisitant(teamName, matches);
    }

    /**
     * CalculateGoalsAgainst shows the goals a team has received, at his field and outside his field
     * @param teamName
     *  Name of the team we want to know how easy is to score them.
     * @param matches
     *  ArrayList with all the games played. To obtain that list we use the class ReadMatches.
     * @return
     *  Returns the unfortunate amount of goals received.
     */
    public static int calculateGoalsAgainst(String teamName, ArrayList<Match> matches){
        int totalGoals = 0;
        for(Match match : matches){
            if(match.getVisitantTeam().getName().equals(teamName)){
                totalGoals += match.getGoalLocal();
            }
            if(match.getLocalTeam().getName().equals(teamName)){
                totalGoals += match.getGoalVisitant();
            }
        }
        return totalGoals;
    }

}