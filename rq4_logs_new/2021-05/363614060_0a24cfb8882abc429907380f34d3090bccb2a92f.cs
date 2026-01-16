using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Linq;

[System.Serializable]
public struct HighScore
{
    public string name;
    public int score;
}

[System.Serializable]
public class HighScores
{
    public string difficultyName;
    public int maxScoreCount = 5;
    public List<HighScore> scores = new List<HighScore>();

    public bool CheckForHighScore(int value)
    {
        if (maxScoreCount > scores.Count)
            return true;
        return scores.Any(x => x.score < value);
    }

    public void AddHighScore(int value, string name)
    {
        AddHighScore(new HighScore { name = name, score = value });
    }

    public void AddHighScore(HighScore value)
    {
        scores.Add(value);
        scores = scores.OrderByDescending(x => x.score).ToList();
        if(scores.Count > maxScoreCount)
        {
            scores.RemoveAt(scores.Count - 1);
        }
    }
}

[System.Serializable]
public class HighScoresList
{
    public List<HighScores> list;

    public HighScores GetByDifficulty(string difficultyName)
    {
        var output = list.FirstOrDefault(x => x.difficultyName == difficultyName);
        if (output == null)
        {
            HighScores newHighScores = new HighScores { difficultyName = difficultyName };
            list.Add(newHighScores);
            output = newHighScores;
        }
        return output;
    }
}

public class HighScoreKeeper : MonoBehaviour
{
    public const string HIGH_SCORES_KEY = "HighScores";
    public HighScoresList highScores;
    public Dictionary<string, HighScores> HighScoresByDifficulty;
    public HighScore testScore;
    public string diffName;

    private void Awake()
    {
        string scoresString = PlayerPrefs.GetString(HIGH_SCORES_KEY);
        Debug.Log(scoresString);
        highScores = JsonUtility.FromJson<HighScoresList>(scoresString);
        HighScoresByDifficulty = highScores.list.ToDictionary(x => x.difficultyName);
        foreach(var key in HighScoresByDifficulty.Keys)
        {
            Debug.Log(HighScoresByDifficulty[key].scores[0].name);
        }

    }
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    [ContextMenu("AddScore")]
    public void TestAdd()
    {
        AddHighScore(testScore, diffName);
    }

    [ContextMenu("GetScore")]
    public void TestGet()
    {
        var test = GetHighScores(diffName);
        foreach (var item in test.scores)
        {
            Debug.Log($"{item.name} : {item.score}");
        }

    }

    public void AddHighScore(HighScore highScore, string difficultyName)
    {
        var highscoresByDifficulty = highScores.GetByDifficulty(difficultyName);
        highscoresByDifficulty.AddHighScore(highScore);
        SaveScores();
    }

    public HighScores GetHighScores(string difficultyName)
    {
        return highScores.GetByDifficulty(difficultyName);
    }

    public bool CheckForHighScore(int value, string difficultyName)
    {
        return highScores.GetByDifficulty(difficultyName).CheckForHighScore(value);
    }

    [ContextMenu("Save")]
    public void SaveScores()
    {
        string scoresString = JsonUtility.ToJson(highScores);
        Debug.Log($"Saving: {scoresString}");
        PlayerPrefs.SetString(HIGH_SCORES_KEY, scoresString);
    }
}