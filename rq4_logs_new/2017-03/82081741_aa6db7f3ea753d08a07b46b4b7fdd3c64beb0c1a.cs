using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class getScoreForShoot : MonoBehaviour
{
    Rigidbody2D rb;
    private int count;
    public Text countText;
    public Text winText;
    public Text finishText;

    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
        count = 0;
        SetCountText();
        winText.text = "";
        finishText.text = "";
    }


    void OnTriggerEnter2D(Collider2D other)
    {
        if (other.gameObject.CompareTag("Pick Up"))
        {
            other.gameObject.SetActive(false);
            count = count + 1;
            SetCountText();
        }

        else if (other.gameObject.CompareTag("Finish"))
        {
            finishText.text = "Sorry.Better Luck Next Time.";
            rb.gameObject.SetActive(false);
        }

    }

    void SetCountText()
    {
        countText.text = "Score: " + count.ToString();
        if (count >= 5)
        {
            winText.text = "Congratulations! Welcome to LSU";
            rb.gameObject.SetActive(false);
        }
    }
}
