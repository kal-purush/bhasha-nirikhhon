package self.rockpaperscissors;

import java.util.Random;

/**
 * Created by sairamsb on 19/11/17.
 */

public class ComputersMove {
    String computerCurrentChoice;
    private String computersMove = "";
    void chooseComputersMove(String playersMove) {
        Random random = new Random();
        int computerChoice = random.nextInt(3) + 1;
        if (computerChoice == 1)
            computerCurrentChoice = "r";
        else if (computerChoice == 2)
            computerCurrentChoice = "p";
        else
            computerCurrentChoice = "s";
        computersMove += computerCurrentChoice;
        System.out.println("**********" + computersMove);
        System.out.println("##########" + playersMove);
    }
}