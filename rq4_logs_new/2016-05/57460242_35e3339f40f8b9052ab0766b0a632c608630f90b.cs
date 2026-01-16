using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class StoryLine : MonoBehaviour {

    public float startingTime;


    public GameObject storyLine;

    public float waitSeconds;

    // Use this for initialization
    void Start()
    {
        startingTime -= Time.deltaTime;

        storyLine.SetActive(true);

    }

    // Update is called once per frame
    void Update()
    {


        if (storyLine.activeSelf)
        {
            waitSeconds -= Time.deltaTime;
        }

        if (waitSeconds < 0)
        {
            SceneManager.LoadScene("Level 1");
        }


    }
}