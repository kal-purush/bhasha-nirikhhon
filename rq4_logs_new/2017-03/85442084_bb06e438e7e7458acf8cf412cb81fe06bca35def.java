import java.util.Random;
import java.util.Scanner;
/**
 * Write a description of class Minigame here.
 * 
 * @author (your name) 
 * @version (a version number or a date)
 */
public class Minigame
{
    // instance variables - replace the example below with your own
    private Random rng;
    private int playerHealth;
    private int killerHealth;
    private Scanner reader;
    private int playerPower;
    private int killerPower;
    

    /**
     * Constructor for objects of class Minigame
     */
    public Minigame()
    {
        playerHealth = 10;
        killerHealth = 10;
        playerPower = 1;
        killerPower = 1;
        rng = new Random();
        reader = new Scanner(System.in);
    }
    public int killerTurn()
    {
        return rng.nextInt(3);
    }
    public int playerTurn()
    {
        System.out.println("What will you do?");
        System.out.println("[1] Strike   [2] Block   [3] Feint   [4] Parry");
        int playerMove = 0;
        while(playerMove == 0){
            String selectedMove = reader.nextLine();
            if(selectedMove.equals("1")){
                System.out.println("You prepare to strike.");
                playerMove = 1;
            }
            else if(selectedMove.equals("2")){
                System.out.println("You get ready to block.");
                playerMove = 2;
            }
            else if(selectedMove.equals("3")){
                System.out.println("You attempt to feint.");
                playerMove = 3;
            }
            else if(selectedMove.equals("4")){
                System.out.println("You try to parry and incoming attack.");
                playerMove = 4;
            }
            else{
                System.out.println("Invalid move. Choose 1, 2, 3, or 4");
            }
        }
        return playerMove;
    }
    public void gameTurn()
    {
        int user = playerTurn();
        int killer = killerTurn();
        if(killer == 0){
            System.out.println("The killer takes a strike at you.");
            if(user == 1){
                System.out.println("Both attacks miss.");
            }
            else if(user == 2){
                System.out.println("You block the attack and land a counter-attack.");
                killerHealth -= playerPower;
            }
            else if(user == 3){
                System.out.println("The killer catches you off guard and strikes you.");
                playerHealth -= killerPower;
            }
            else if(user == 4){
                System.out.println("You parry the killer's strike and they stumble. You sweep the killer's leg and make a critical strike");
                killerHealth -= 2 * playerPower;
            }
        }
        else if(killer == 1){
            System.out.println("The killer tries to block.");
            if(user == 1){
                System.out.println("The killer blocks your strike and counters it.");
                playerHealth -= killerPower;
            }
            else if(user == 2){
                System.out.println("Neither of you move.");
            }
            else if(user == 3){
                System.out.println("The killer is caught off guard by your bluff and you land a strike.");
                killerHealth -= playerPower;
            }
            else if(user == 4){
                System.out.println("The killer strikes you.");
                playerHealth -= killerPower;
            }
        }
        else if(killer == 2){
            System.out.println("The killer attempts to feint.");
            if(user == 1){
                System.out.println("The killer is caught off guard by your strike.");
                killerHealth -= playerPower;
            }
            else if(user == 2){
                System.out.println("You fall for the killer's bluff and take a hit.");
                playerHealth -= killerPower;
            }
            else if(user == 3){
                System.out.println("Neither of your bluffs succeed.");
            }
            else if(user == 4){
                System.out.println("The killer strikes you.");
                playerHealth -= killerPower;
            }
        }

    }
    public void killerBattle()
    {
        boolean fight = true;
        System.out.println("A wild killer appears!");
        while(fight){
            gameTurn();
            System.out.println("Your Health: " + playerHealth + "  Killer's Health: " + killerHealth);
            if(playerHealth <= 0){
                System.out.println("You died.");
                fight = false;
            }
            else if(killerHealth <= 0){
                System.out.println("You defeated the killer!");
                fight = false;
            }
        }
    }

    /**
     * An example of a method - replace this comment with your own
     * 
     * @param  y   a sample parameter for a method
     * @return     the sum of x and y 
     */

}