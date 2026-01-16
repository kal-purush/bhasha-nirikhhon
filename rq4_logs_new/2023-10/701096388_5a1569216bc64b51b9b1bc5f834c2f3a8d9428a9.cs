using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HelmController : MonoBehaviour
{
    public float TimeLeft;
    public bool TimerOn;

    public Text TimerText;

    // Start is called before the first frame update
    void Start()
    {
        TimeLeft = 240.0f; //4 minutes
        TimerOn = true;
    }

    // Update is called once per frame
    void Update()
    {
        if (TimerOn)
        {
            if (TimeLeft > 0)
            {
                TimeLeft -= Time.deltaTime;
            }
            else
            {
                TimeLeft = 0;
                TimerOn = false;
            }
        }
        TimerText.text = "Countdown: " + TimeLeft.ToString("F0");
    }
    
    private void OnTriggerEvent2D(Collider2D collision)
    {
        Debug.Log(collision.gameObject.tag);

        if (collision.gameObject.tag == "Player")
        {
            Debug.Log("IM DRIVING");
            TimeLeft -= Time.deltaTime;
        }
    }
}
