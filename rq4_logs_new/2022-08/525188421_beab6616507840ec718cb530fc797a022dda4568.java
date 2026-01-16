/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package blackjack;

import casino.Player;
import constants.Constants;
import java.util.Scanner;
import java.util.ArrayList;


/**
 *
 * @author minab
 */
public class BlackJack {
    
    // add member variables 
    Player player;
    boolean play;
    Scanner scan;
    Deck deck;
    Dealer dealer;
    Card card;
    // make constructor for it similar to slots
    public BlackJack (Player player)  {
           this.player = player;
           scan = new Scanner(System.in);
           play = true;
           // instantiating dealer, deck and card
           deck =  new Deck();
           dealer = new Dealer();
           card = new Card();
    }
    
    // make play method to call displayRules method 
    public void play()     {
           displayRules();
                if(player.getCash() < Constants.BLACK_JACK_BET) {
                    System.out.println("You don't have enough cash to play SORRY!");
                    play = false;
                }
          while (play) {
                // reset deck so it doesn't run out of cards
                deck =  new Deck();
                
                System.out.println("");   
                 System.out.println("Your cash balance is $" +player.getCash());
                       if(player.getCash() < Constants.BLACK_JACK_BET) {
                           System.out.println("You don't have enough cash to play SORRY!");
                           play = false;
                           break;
                       }

              // Deduct BlackJack fee from player's cash
              player.setCash(player.getCash() - Constants.BLACK_JACK_BET);

             System.out.println("Dealing cards ....");
              // reset player's score, busted, and hand 0, false, and new ArrayList
              player.setHand(new ArrayList <Card>());
              player.setScore(Constants.ZERO);
              player.setBusted(false);

              // reset dealer's score and hand 0 and new ArrayList
              dealer.setHand(new ArrayList<Card>());
              dealer.setScore(Constants.ZERO);

              // deal the player two cards and output the player's score 
               for(int dealP = 0; dealP<2; dealP++) {
                       card = dealer.deal(deck);
                       System.out.println("You got "+ card.toString());
                       player.getHand().add(card);  
               }
              System.out.println("Your score is " +player.getScore());

              // deal the dealer two cards to start new game 
               for(int dealD= 0; dealD <2; dealD++) {
                   card = dealer.deal(deck);
                   dealer.getHand().add(card);  
               }
              // if the player hits, deal a card to him, output his score and check if he busted or not
               while(player.hitOrStand() == Constants.HIT)    {
                        card = dealer.deal(deck);
                        System.out.println("You got "+ card.toString());
                        player.getHand().add(card);  
                        System.out.println("Your score is "+ player.getScore());

                       // if player busted, game over and break out of loop
                       if(player.getScore() > Constants.BUST)  {
                               System.out.println("YOU BUSTED! YOU LOST THE HAND! GAME OVER!!!");
                               player.setBusted(true); 
                               break;
                        }
               }
              // if player busted ask him to play again otherwise see if dealer busted
               if(player.isBusted())  {
                   play = player.playAgain();
                   continue;                                        
               }

              else {
                  System.out.println("The dealer's score before his turn is " +dealer.getScore());

                  // while  the dealer's score is smaller than 16, give dealer another card
                  while(dealer.getScore() < Constants.DEALER_HIT)  {
                        dealer.hitOrStand();
                        card = dealer.deal(deck);
                        dealer.getHand().add(card);  
                        System.out.println("The dealer's score after his turn is " +dealer.getScore());

                       // check  to see if the dealer busted and if so tell player and break
                        if(dealer.getScore() > Constants.BUST) {
                                   System.out.println("Dealer busted");
                                   break;
                        }
                  }
             }
               results();
               play = player.playAgain();
          }
    }
    private void results()   {
           System.out.println("Your score is "+ player.getScore());
           System.out.println("Dealer's score is "+ dealer.getScore());
           
           // award money based on score
           // if dealer busts, player wins $10
           if(dealer.getScore() > Constants.BUST)  {
                System.out.println("Dealer busted");
                System.out.println("You won $"+Constants.DEALERBUST_TIED_PWINS);
                player.setCash(player.getCash() + Constants.DEALERBUST_TIED_PWINS);
           }
           // if dealer ties with player, player wins $10
           else if(dealer.getScore() == player.getScore()) {
               System.out.println("It was a PUSH!!");
               System.out.println("You win $" + Constants.DEALERBUST_TIED_PWINS);
               player.setCash(player.getCash() + Constants.DEALERBUST_TIED_PWINS);
           }
           // if player's score is higher, player wins and gets $20
           else if(player.getScore() > dealer.getScore()) {
               System.out.println("YOU WON THE HAND, YOU GET $" + Constants.BLACK_JACK_WIN);
               player.setCash(player.getCash() + Constants.BLACK_JACK_WIN);
           }
           // otherwise dealer wins
           else {
               System.out.println("DEALER WINS the hand, Maybe next time");
           }

    }
  // make private method to display Black Jack rules
    private void displayRules()    {
        System.out.println("Let's play Black Jack!");
        System.out.println("Your cash balance is $" + player.getCash());   // add cash balance 
        System.out.println("The bet is $" + Constants.BLACK_JACK_BET);
        System.out.println("Dealer must HIT if their score is 16 or less");
        System.out.println("Dealer must STAND if their score is 17 or higher");
        System.out.println("If the player wins the hand, they receive $" +Constants.BLACK_JACK_WIN);
        System.out.println("If the dealer wins the hand, the $" +Constants.BLACK_JACK_BET + " bet is lost");
        System.out.println("If it is a PUSH, the player keeps their $"+Constants.BLACK_JACK_BET + " bet");
    }
}