/**
 * Created by dhnishi on 4/11/15.
 */

enum Suit {
    SPADES,
    HEARTS,
    DIAMONDS,
    CLUBS
}

enum Rank {
    ZERO_UNUSED,
    ONE_UNUSED,
    TWO,
    THREE,
    FOUR,
    FIVE,
    SIX,
    SEVEN,
    EIGHT,
    NINE,
    TEN,
    JACK,
    QUEEN,
    KING,
    ACE
}

class Card {
    suit : Suit;
    rank : Rank;
    constructor(suit, rank) {
        this.suit = suit;
        this.rank = rank;
    }

    static compareTo(a : Card, b : Card) {
        if (a.rank > b.rank) {
            return 1;
        }
        if (a.rank < b.rank) {
            return -1;
        }
        return 0;
    }
}