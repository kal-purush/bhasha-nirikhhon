using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;
using UnityEngine.SceneManagement;

public class GameManager : MonoBehaviour
{
    public static GameManager Instance;

    public static UnityAction m_GameEnd;
    public static UnityAction m_Death;

    public bool isLoadNewScene = false;

    [SerializeField] private string endSceneName = "Main Menu";
    [SerializeField] private string deathSceneName = "Main Menu";

    // Start is called before the first frame update
    void Awake()
    {
        if (!Instance) Instance = this;
    }

    private void Start()
    {
        m_Death += Death;
        m_GameEnd += GameEnd;
    }

    private void GameEnd()
    {
        isLoadNewScene = true;
        SceneManager.LoadScene(endSceneName);
    }

    private void Death()
    {
        isLoadNewScene = true;
        StartCoroutine(nameof(DeathSceneLoad));
    }

    IEnumerator DeathSceneLoad()
    {
        for (int i = 0; i < 20; i++)
        {
            if(i >= 19) SceneManager.LoadScene(deathSceneName, LoadSceneMode.Single);
            yield return null;
        }
    }
}