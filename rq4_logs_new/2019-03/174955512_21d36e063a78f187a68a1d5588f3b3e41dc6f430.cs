using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HexDeck
{
    const int DeckSize = 500;
    HexColor[] hexDeck;
    int index;

    public HexDeck() {
        hexDeck = new HexColor[DeckSize];
    }

    public void MakeDeck(){
        var colorNum = (int)HexColor.size;
        for (int i=0; i<hexDeck.Length; ++i){
            hexDeck[i] = (HexColor)Random.Range(0, colorNum);
        }
    }

    public HexColor Pop(){
        var target = hexDeck[index];
        index++;
        if(index >= hexDeck.Length){
            index = 0;
        }
        return target;
    }
}