package edu.oregonstate.cs361.battleship;

/**
 * Created by michaelhilton on 1/26/17.
 */
public class BattleshipModel {
  //class ship names
  public Ship player_aircraftCarrier;
  public Ship player_battleship;
  public Ship player_cruiser;
  public Ship player_destroyer;
  public Ship player_submarine;
  public Ship computer_aircraftCarrier;
  public Ship computer_battleship;
  public Ship computer_cruiser;
  public Ship computer_destroyer;
  public Ship computer_submarine;

  //class
  public ArrayList<coord> player_hits;
  public ArrayList<coord> player_misses;
  public ArrayList<coord> computer_hits;
  public ArrayList<coord> computer_misses;

  public BattleshipModel() {

    this.player_aircraftCarrier = new Ship("AircraftCarrier", 5, new coord(0, 0), new coord(0, 0));
    this.player_battleship = new Ship("Battleship", 4, new coord(0, 0), new coord(0, 0));
    this.player_cruiser = new Ship("Cruiser", 3, new coord(0, 0), new coord(0, 0));
    this.player_destroyer = new Ship("Destroyer", 2, new coord(0, 0), new coord(0, 0));
    this.player_submarine = new Ship("Submarine", 2, new coord(0, 0), new coord(0, 0));

    this.computer_aircraftCarrier = new Ship("Computer_AircraftCarrier", 5, new coord(2, 2), new coord(2, 7));
    this.computer_battleship = new Ship("Computer_Battleship", 4, new coord(2, 8), new coord(6, 8));
    this.computer_cruiser = new Ship("Computer_Cruiser", 3, new coord(4, 1), new coord(4, 4));
    this.computer_destroyer = new Ship("Computer_Destroyer", 2, new coord(7, 3), new coord(7, 5));
    this.computer_submarine = new Ship("Computer_Submarine", 2, new coord(9, 6), new coord(9, 8));

    this.player_hits = new ArrayList<>();
    this.player_misses = new ArrayList<>();
    this.computer_hits = new ArrayList<>();
    this.computer_misses = new ArrayList<>();

  }

}