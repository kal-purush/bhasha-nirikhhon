using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Leader
{
    public string name;
    public int score;
    
    public Leader (string username, int currency)
    {
        name = username;
        score = currency;
    }

    public int CompareTo(Leader other)
    {
        if (other == null)
        {
            return 1;
        }

        return score - other.score;
    }

}

public static class Leaderboard
{
    public static List<Leader> leaders = new List<Leader>();
}