package CrapsApp;

import java.util.Random;

public class Craps {
    private Random random = new Random();

    private enum Status{CONTINUE, WON, LOST};

    private final static int SNAKE_EYES = 2;
    private final static int TREY = 3;
    private final static int SEVEN = 7;
    private final static int YO_ELEVEN = 11;
    private final static int BOX_CARS = 12;

    public void play(){
        int myPoint = 0;
        Status gameStatus;
        int sumOfDice = rollDice();

        switch (sumOfDice){
            case SEVEN:
            case YO_ELEVEN:
                gameStatus = Status.WON;
                break;
            case SNAKE_EYES:
            case TREY:
            case BOX_CARS:
                gameStatus = Status.LOST;
                break;
                default:
                    gameStatus = Status.CONTINUE;
                    myPoint = sumOfDice;
                    System.out.printf("Point is %d\n", myPoint);
                    break;
        } // end of switch statement

        while(gameStatus == Status.CONTINUE){
            sumOfDice = rollDice();

            if(sumOfDice == myPoint)
                gameStatus = Status.WON;
        }//end of while loop

        if(gameStatus == Status.WON)
            System.out.println("Player wins!");
        else
            System.out.println("Player loses!  :(");
    }

    public int rollDice(){
        int die1 = 1 + random.nextInt(6);
        int die2 = 1 + random.nextInt(6);

        int sum = die1 + die2;

        System.out.printf("Player rolled %d + %d = %d\n", die1, die2, sum);
        return  sum;
    }
}