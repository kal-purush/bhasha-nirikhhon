using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class initialScore : MonoBehaviour
{

    Rigidbody2D rb;
    private int count;
    public Text currentScoreText;

    void Start()
    {
        currentScoreText = GetComponent<Text>();
        count = 0;
        //SetCountText();
        currentScoreText.text = "Score: " + count.ToString();
    }

    
}