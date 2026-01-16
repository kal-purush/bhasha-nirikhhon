using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UIController : MonoBehaviour
{
    public GameObject ball;
    public GameObject planet;
    public GameObject UI;
    public GameObject GameOverScr;
    public GameObject Victory;

    private Vector2 stop;

    public void Start()
    {
        UI.SetActive(true);
        GameOverScr.SetActive(false);
        Victory.SetActive(false);
        stop = new Vector2(0, 0);
    }

    public void FixedUpdate()
    {
        if(ball == null)
        {
            UI.SetActive(false);
            GameOverScr.SetActive(true);
        }
        
        if(planet == null)
        {
            UI.SetActive(false);
            ball.GetComponent<Rigidbody2D>().gravityScale = 0;
            ball.GetComponent<Rigidbody2D>().velocity = stop;
            Victory.SetActive(true);
        }
    }
}