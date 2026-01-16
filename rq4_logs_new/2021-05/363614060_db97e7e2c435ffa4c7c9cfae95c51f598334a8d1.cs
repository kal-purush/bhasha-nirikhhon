using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[CreateAssetMenu(menuName = "Game Session", fileName = "New Game Session")]
public class GameSession : ScriptableObject
{
    public int score;
    public string currentDifficulty;
    public bool isGameStarted = false;

    public void ResetGameSession(string difficultyName)
    {
        currentDifficulty = difficultyName;
        score = 0;
    }
    public void ResetGameSession()
    {
        score = 0;
    }
}


