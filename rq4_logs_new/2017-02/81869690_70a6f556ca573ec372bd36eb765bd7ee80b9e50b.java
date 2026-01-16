package com.mark;

import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by hl4350hb on 2/15/2017.
 */
public class adv_prob_2 {
        // Creates a Random object to randomly pick numbers
        static Random random = new Random();
        // Creates a Scanner object
        static Scanner numberScanner = new Scanner(System.in);

    public static void main(String[] args) {
        // Creates variable holding number of cards each player will receive
        int cards_per_Player = 5;
        // Creates string array to hold the player's cards
        String[] players_cards = new String[cards_per_Player];
        // Loops through variable and randomly selects number
        for (int i = 0; i < cards_per_Player; i++) {
            // To make it simple, the suite is determined by the range
            // the number picked is in. Also, the face cards are
            // represented as a number for now.
            int card = random.nextInt(52);
            if (card <= 12) {
                players_cards[i] = String.format("%d of Spades", card + 1);
            }
            // Number subtracts a multiple of 13 (number of cards per suite) and
            // adds one to offset the base-0.
            else if (card >= 13 && card <= 25) {
                players_cards[i] = String.format("%d of Diamonds", (card - 13) + 1);
            }
            else if (card >= 26 && card <= 38) {
                players_cards[i] = String.format("%d of Clubs", (card - 26) + 1);
            }
            else if (card >= 39) {
                players_cards[i] = String.format("%d of Hearts", (card - 39) + 1);
            }
        }
        // Displays all cards in the player's hand.
        System.out.println(Arrays.toString(players_cards));
        // Prompts user for card number they are looking for
        System.out.println("What cards are you looking for? (1-13)");
        int wanted_cards = numberScanner.nextInt();
        // Displays generic response.
        System.out.println("I don't have any of those. GO FISH!");
    }
}
