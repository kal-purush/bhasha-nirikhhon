using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameInitializer : MonoBehaviour
{
    public PlayerStats playerStats;
    [SerializeField] private GameObject player;
    private static GameInitializer _instance;
    void Awake()
    {

        if (_instance == null)
        {
            _instance = this;
            playerStats.InitStats();
        }
        else
        {
            Debug.LogWarning("There is already an instance of GameInitializer in the scene");
        }

        DontDestroyOnLoad(this.gameObject);
    }

    void Start()
    {
        player = GameObject.FindWithTag("Player");
    }

}