/*using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PauseGame : MonoBehaviour
{

    public Transform Canvas;
    public Transform Player;


    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.Escape))
        {
            Pause();
        }
    }
    public void Pause()
    {

        if (Canvas.gameObject.activeInHierarchy == false)
        {
            Canvas.gameObject.SetActive(true);
            Time.timeScale = 0;
            Player.GetComponent<Player>().enabled = false;

        }
        else
        {
            Canvas.gameObject.SetActive(false);
            Time.timeScale = 1;
            Player.GetComponent<Player>().enabled = true;
        }
    }
}*/