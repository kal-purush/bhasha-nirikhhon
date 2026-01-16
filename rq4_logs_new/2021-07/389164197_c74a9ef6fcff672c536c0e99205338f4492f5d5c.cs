using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ButtonController : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void PlayGame()
    {
        EventBroadcaster.Instance.PostEvent(EventNames.ON_START_GAME);
    }

    public void QuitGame()
    {
        EventBroadcaster.Instance.PostEvent(EventNames.ON_QUIT_GAME);
    }

    public void PauseGame()
    {
        EventBroadcaster.Instance.PostEvent(EventNames.ON_PAUSE_GAME);
    }

    public void ResumeGame()
    {
        EventBroadcaster.Instance.PostEvent(EventNames.ON_RESUME_GAME);
    }

    public void QuitToMenu()
    {
        EventBroadcaster.Instance.PostEvent(EventNames.ON_QUIT_TO_MENU);
    }



}