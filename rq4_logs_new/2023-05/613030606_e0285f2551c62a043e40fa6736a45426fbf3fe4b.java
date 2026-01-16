package ch.uzh.ifi.hase.soprafs23.rest.dto;

public class UserStatisticsGetDTO {

    private String username;
    

    private int totalPoints;
    private int totalGamesPlayed;

    /* 
     * Only getters and setters below this point
     */

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getTotalPoints() {
        return totalPoints;
    }

    public void setTotalPoints(int totalPoints) {
        this.totalPoints= totalPoints;
    }

    public int getTotalGamesPlayed() {
        return totalGamesPlayed;
    }

    public void setTotalGamesPlayed(int totalGamesPlayed) {
        this.totalGamesPlayed = totalGamesPlayed;
    }
    
}