/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tictactoe.domain.model;

/**
 *
 * @author LeaderTech
 */
public class Record {
    private String user1;
    private String user2;
    private String psitions;
    private char winner;

    public String getUser1() {
        return user1;
    }

    public void setUser1(String user1) {
        this.user1 = user1;
    }

    public String getUser2() {
        return user2;
    }

    public void setUser2(String user2) {
        this.user2 = user2;
    }

    public String getPsitions() {
        return psitions;
    }

    public void setPsitions(String psitions) {
        this.psitions = psitions;
    }

    public char getWinner() {
        return winner;
    }

    public void setWinner(char winner) {
        this.winner = winner;
    }



    
}