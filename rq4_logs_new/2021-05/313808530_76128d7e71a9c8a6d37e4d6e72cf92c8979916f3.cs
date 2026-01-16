using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Playables;
using UnityEngine.SceneManagement;

public class MemoryEndTL : MonoBehaviour
{
    private GameObject TimeLine;

    void Awake()
    {
        TimeLine = GameObject.Find("LV3EndTimeline");
    }

    void Start()
    {
        TimelineGameManager.GetDirector(TimeLine.GetComponent<PlayableDirector>());
        TimelineGameManager.isTimeline = true;
    }

    void Update()
    {

    }

    public void LV3EndTimeline()
    {
        TimelineGameManager.isTimeline = false;
        LevelLoader.instance.LoadLevel("Level3Part2");
        // TimeLine.GetComponent<PlayableDirector>().enabled = false;
        // StartCoroutine(StartNextLevel());
    }

    IEnumerator StartNextLevel()
    {
        yield return new WaitForSeconds(1f);
        LevelLoader.instance.LoadLevel("Level3Part2");
    }
}