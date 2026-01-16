package kr.ac.cnu.games.poker;

import lombok.Data;

/**
 * Created by rokim on 2017. 6. 4..
 */
@Data
public class Card {
    private final int number;
    private final Suit suit;

    public Card(int number, Suit suit) {
//        if (number > 13) {
//            throw new NoSuchRankException();
//        }

        this.number = number;
        this.suit = suit;
    }
}