using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ScoreAndLife : MonoBehaviour
{
    #region Singleton
    public static ScoreAndLife Instance;
    private void Awake()
    {
        Instance = this;
    }
    #endregion

    public int score;
    public int nextLife;
    public GameObject Player;
    void Start()
    {
        score = 0;
    }

    void Update()
    {
        if(score >= nextLife)
        {
            nextLife = nextLife * 3;
            Player.GetComponent<PlayerController>().life++;
        }
    }
}