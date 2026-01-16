using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class Timer : MonoBehaviour
{
    public float minutes;
    public float seconds;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        print ("Осталось:" +minutes+ ":" + seconds);
        seconds -= Time.deltaTime;
        if (seconds <= 0) 
        {
            minutes -= 1;
            if (minutes <= 0)
            {
                int sceneIndex = SceneManager.GetActiveScene().buildIndex;
                SceneManager.LoadScene(sceneIndex);
            }   
            else
            {
                seconds += 60;
            }
        }
    }
}