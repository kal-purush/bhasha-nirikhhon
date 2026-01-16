/**
 * Created by kegan on 5/1/15.
 */
public class Card {
    int rank, suit;
    public Card(int rank, int suit){
        this.rank = rank;
        this.suit = suit;
    }
    public String getRank(){
        String result;
        switch (rank){
            case 1:
                result = "Ace";
                break;
            case 2:
                result = "Two";
                break;
            case 3:
                result = "Three";
                break;
            case 4:
                result = "Four";
                break;
            case 5:
                result = "Five";
                break;
            case 6:
                result = "Six";
                break;
            case 7:
                result = "Seven";
                break;
            case 8:
                result = "Eight";
                break;
            case 9:
                result = "Nine";
                break;
            case 10:
                result = "Ten";
                break;
            case 11:
                result = "Jack";
                break;
            case 12:
                result = "Queen";
                break;
            case 13:
                result = "King";
                break;
            default:
                result = "Joker";
                break;
        }
        return result;
    }
    public String getSuit(){
        String result;
        switch(suit){
            case 1:
                result = "Spades";
                break;
            case 2:
                result = "Hearts";
                break;
            case 3:
                result = "Diamonds";
                break;
            case 4:
                result = "Clubs";
                break;
            default:
                result = "Gems";
                break;
        }
        return result;
    }
}