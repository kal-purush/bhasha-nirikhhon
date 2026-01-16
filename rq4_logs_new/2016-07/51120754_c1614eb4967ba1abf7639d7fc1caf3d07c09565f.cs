using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class CodexDisplayCanvas : MonoBehaviour {

    public Pause pauseMenu;
	void Start()
    {
        //this.gameObject.SetActive(false);
    }

    
    
    //set the codex text to display
    public void SetText(string codexText)
    {
        //prevent pause from overlapping with codex pause
        pauseMenu.busy = true;
        //pause game
        Time.timeScale = 0;
        //set codex text
        gameObject.GetComponentInChildren<Text>().text = codexText;

    }
    public void CloseCodex()
    {
        
       //unpause game
        Time.timeScale = 1;
        //reactivate pause menu
        pauseMenu.busy = false;
        //deactivate this canvas
        this.gameObject.SetActive(false);
    }

}