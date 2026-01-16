package model;

import java.util.ArrayList;
import java.util.List;

public class UserInfo {

  private String username;
  private String email;
  private boolean online;
  private List<String> friends;
  private int rank;
  private int wins;
  private int loses;
  private int draws;

  public UserInfo(String username, String email) {
    this.username = username;
    this.email = email;
    online = true;
    friends = new ArrayList<>();
    rank = 0;
    wins = 0;
    loses = 0;
    draws = 0;
  }

  public UserInfo(User user) {
    this.username = user.getUsername();
    this.email = user.getEmail();
    this.online = user.isOnline();
    this.friends = user.getFriends();
    this.rank = user.getRank();
    this.wins = user.getWins();
    this.loses = user.getLoses();
    this.draws = user.getDraws();
  }

  public String getUsername() {
    return username;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public void setOnline() {
    this.online = true;
  }

  public void setOffline() {
    this.online = false;
  }

  public void addFriend(String friend) {
    friends.add(friend);
  }

  public void setRank(int rank) {
    this.rank = rank;
  }

  public void addWin() {
    wins++;
  }

  public void addLoss() {
    loses++;
  }

  public void addDraw() {
    draws++;
  }
}