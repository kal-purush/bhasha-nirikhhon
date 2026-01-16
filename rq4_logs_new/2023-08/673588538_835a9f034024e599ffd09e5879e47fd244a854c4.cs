using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ScoreableObject : MonoBehaviour
{
    // This script is added to any oject that can score points
    public int scoreValue;
    private int defaultScoreValue = 25;

    void Start ()
    {
        if(scoreValue == 0){
            scoreValue = defaultScoreValue;
        }
    }

    private void OnCollisionEnter(Collision collision)
    {
         // If collision with a ball object
        if (collision.gameObject.tag == "ball")
        {
            GameManager.Instance.Score += scoreValue;
        }
    }
}