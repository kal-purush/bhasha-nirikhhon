package nz.ac.auckland.se281;

import java.util.List;

public abstract class Ai {

  private Strategy strategy;

  // get sum and fingers methods are the same for easy, medium, hard or master
  public int getSumAi(List<Integer> humanFingersCount) {
    return strategy.getSumAi(humanFingersCount);
  }

  public int getFingersAi() {
    return strategy.getFingersAi();
  }

  // setStrategy for all subclasses
  public void setStrategy(Strategy strategy) {
    this.strategy = strategy;
  }

  // Each subclass has a different implementation of strategy
  public abstract void chooseStrategy(int counter);
}