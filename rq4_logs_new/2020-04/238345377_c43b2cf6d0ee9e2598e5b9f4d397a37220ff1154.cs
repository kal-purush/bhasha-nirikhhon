using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour
{
    GameManager instance;

    int level = 0;
    int difficulty = 1;
    float overallDifficulty;
    static int stats = 5;
    //Strength/Agility/Intelligence/Endurance/Wisdom
    float[] playerStats = new float[stats];
    int baseStat = 5;
    int playerLevel = 1;
    // Start is called before the first frame update
    void Awake()
    {
        if (instance == null)
        {
            instance = this;
        }
        else
        {
            Destroy(this.gameObject);
        }
        DontDestroyOnLoad(this.gameObject);
        for(int i = 0; i < stats; i ++)
        {
            playerStats[i] = baseStat;
        }
    }

    void calculateDifficulty()
    {
        if (level > 10)
            overallDifficulty = difficulty + level;
        else
            overallDifficulty = ((difficulty + level) * (difficulty + level))/difficulty;
    }
    public void Difficulty(int i)
    {
        difficulty = i;
    }
    public void increaseLevel()
    {
        playerLevel++;
    }
    public int getPlayerLevel()
    {
        return playerLevel;
    }
    public void clearStats()
    {
        for (int i = 0; i < stats; i++)
            playerStats[i] = baseStat;
    }
    public void increaseStrength(int i)
    {
        playerStats[0] += i;
    }
    public void increaseAgility(int i)
    {
        playerStats[1] += i;
    }
    public void increaseIntelligence(int i)
    {
        playerStats[2] += i;
    }
    public void increaseEndurance(int i)
    {
        playerStats[3] += i;
    }
    public void increaseWisdom(int i)
    {
        playerStats[4] += i;
    }
    public float getStrength()
    {
        return playerStats[0];
    }
    public float getAgility()
    {
        return playerStats[1];
    }
    public float getIntelligence()
    {
        return playerStats[2];
    }
    public float getEndurance()
    {
        return playerStats[3];
    }
    public float getWisdom()
    {
        return playerStats[4];
    }
    public float getDifficulty()
    {
        return overallDifficulty;
    }
}