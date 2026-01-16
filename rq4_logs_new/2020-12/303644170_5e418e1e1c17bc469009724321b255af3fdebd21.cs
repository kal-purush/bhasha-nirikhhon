using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class PauseGame : MonoBehaviour
{
    public bool paused = false;
    public GameObject pauseMenu;

    // Update is called once per frame
    void Update()
    {
        if(Input.GetButtonDown("Cancel") || Input.GetKeyDown(KeyCode.P)){
            if(paused == false){
                Pause(true, 0);
            } else{
                Pause(false, 1);
            }
        }
    }

    public void UnpauseGame(){
        Pause(false, 1);
    }

    public void RestartGame(){
        // TODO: restart game
        SceneManager.LoadScene("SampleScene");
        Pause(false, 1);
    }

    private void Pause(bool pausing, int timescale){
        Time.timeScale = timescale;
        paused = pausing;
        Cursor.visible = pausing;
        pauseMenu.SetActive(pausing);
    }
}