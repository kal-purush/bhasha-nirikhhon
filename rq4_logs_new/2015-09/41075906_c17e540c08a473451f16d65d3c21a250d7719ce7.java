package com.dota.chinanaive.entity;

public class HeroRecord {
  private int heroid;
  private int useCount;
  private int winCount;
  public int getHeroid() {
    return heroid;
  }
  public void setHeroid(int heroid) {
    this.heroid = heroid;
  }
  public int getUseCount() {
    return useCount;
  }
  public void setUseCount(int useCount) {
    this.useCount = useCount;
  }
  public int getWinCount() {
    return winCount;
  }
  public void setWinCount(int winCount) {
    this.winCount = winCount;
  }
  
  public String toString() {
    return "Heroid:" + heroid + " win count:" + winCount + " use count:" + useCount +"\n";
  }
}