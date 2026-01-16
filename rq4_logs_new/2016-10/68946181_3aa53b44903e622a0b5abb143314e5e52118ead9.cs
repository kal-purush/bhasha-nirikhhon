using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class HUDUI : MonoBehaviour {

    public bool gamePaused;
    public Graphic fader;
    public Text gameName;
    public Text pauseText;

    public void Pause()
    {
        if (!gamePaused)
        {
            Time.timeScale = 0;
            fader.enabled = true;
            gameName.enabled = true;
            pauseText.enabled = true;
        }
        else
        {
            Time.timeScale = 1;
            fader.enabled = false;
            gameName.enabled = false;
            pauseText.enabled = false;
        }

        gamePaused = !gamePaused;
    }
}