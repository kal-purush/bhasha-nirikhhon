package project2;

import java.util.ArrayList;

/**
 * Created by sehong on 5/4/15.
 */
public class AI {
    ArrayList<Card> hand;
    Deck deck;
    int drawOption;

    //default constructor
    AI(int drawOption, Deck deck){
        this.drawOption = drawOption;
        this.deck = deck;
        hand = new ArrayList<Card>();

    }

    //adding a card to the hand
    void add(Card card){
        hand.add(card);
    }
    //produce a list of possible play options
    ArrayList<Integer> playOptions(int suit, int rank){
        ArrayList<Integer> options = new ArrayList<Integer>();
        for(int i = 0 ; i < hand.size();i++){
            Card temp = hand.get(i);
            if(temp.suit == suit || temp.rank == rank || temp.rank == 8){
                options.add(i);
            }
        }
        return options;
    }
    public static String displayHand(ArrayList<Card> hand){
        String result = "AI's hand is currently:\n";
        for(int i = 0 ; i < hand.size();i++){
            result += hand.get(i).getName() + "    ";
        }
        return result;
    }

    //automatically play a card from the given options
    int playCard(int suit, int rank){
        ArrayList<Integer> options;
        int drawCount =0;
        System.out.println(displayHand(hand));
        options = playOptions(suit, rank);

        //operation for draw
        while(options.size()==0 && (drawCount < drawOption || drawOption <= 0)) {
            System.out.println("AI doesn't have any card to play");
            System.out.println("AI drawing");
            add(deck.draw());
            drawCount++;
            System.out.println(displayHand(hand));
            options = playOptions(suit, rank);

        }

        //Check draw count to determine action
        if(drawCount >= drawOption && options.size()==0 && drawOption > 0 ) {
            //skip a turn
            System.out.println("Maximum draw count reached. AI is skipping this turn\n");
            return 0;
        }else {
            //play a card
            int randomChoice = (int) (Math.random() * options.size()); // random choice from 0 to options.size()
            Card playCard = hand.get(options.get(randomChoice));
            System.out.println("AI played: " + playCard.getName());
            hand.remove((int) options.get(randomChoice));
            deck.discard(playCard);
            if(playCard.rank == 8){
                return (int)(Math.random()*4+1);
            }else{
                return -1; //not a eight
            }

        }

    }
    boolean checkWin(){
        return hand.size() == 0;
    }





}