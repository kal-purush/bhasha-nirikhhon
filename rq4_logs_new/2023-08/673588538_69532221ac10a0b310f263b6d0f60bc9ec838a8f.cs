using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour
{
    // Stores the instance of the GameManager
    public static GameManager Instance;

    // Stores the game score
    public int Score;

    private void Awake()
    {
        // Singleton to prevent multiple GameManagers from existing
        if(Instance != null)
        {
            Destroy(gameObject);
            return;
        }
        Instance = this;
        DontDestroyOnLoad(gameObject);
    }

    void Start()
    {
        Score = 0;
        InvokeRepeating("PrintScore", 2f, 2f);
    }

    void PrintScore()
    {
        Debug.Log("Score: " + Score);
    }
}