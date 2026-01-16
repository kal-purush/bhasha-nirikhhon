using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerScore : IComparable<PlayerScore>
{
    public int playerID;
    public int score;
    
    public PlayerScore(int _playerID)
    {
        playerID = _playerID;
        score = 0;
    }

    public int CompareTo(PlayerScore other)
    {
        if(this.score > other.score)
        {
            return -1;
        } else if (this.score == other.score)
        {
            return 0;
        } else
        {
            return 1;
        }
    }


}