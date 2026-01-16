using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Playables;
using UnityEngine.SceneManagement;

public class GirlEscape : MonoBehaviour
{
    private GameObject TimeLine;

    void Awake()
    {
        TimeLine = GameObject.Find("GirlFallTimeline");
    }
    // Start is called before the first frame update
    void Start()
    {
        TimelineGameManager.GetDirector(TimeLine.GetComponent<PlayableDirector>());
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}