package com.bau_hornick.blackjack;

/**
 * Created by tennkarene on 1/30/2017.
 */

public class Card {
    private int image;
    private int value;
    private String suit;

    public String getSuit()
    {
        return suit;
    }

    public int getValue()
    {
        return value;
    }

    public int getImage()
    {
        return image;
    }
}