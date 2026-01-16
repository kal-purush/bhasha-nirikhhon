using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class GameTimerScript : MonoBehaviour
{
    //Variables
    public float timeLeft = 900.0f;
    public Text text;
    bool clock;
    private float mins;
    private float secs;

    void Update()
    {
        if (timeLeft > 0 && clock == false)
        {
            clock = true;
            StartCoroutine(Wait());
        }

    }

    IEnumerator Wait()
    {
        timeLeft -= 1;
        UpdateTimer();
        yield return new WaitForSeconds(1);
        clock = false;
    }

    void UpdateTimer()
    {
        int min = Mathf.FloorToInt(timeLeft / 60);                                                          // Displays the time in Minutes:Seconds.
        int sec = Mathf.FloorToInt(timeLeft % 60);                                                          // Displays the time in Minutes:Seconds.
        text.GetComponent<UnityEngine.UI.Text>().text = min.ToString("00") + ":" + sec.ToString("00");      
    }
}
