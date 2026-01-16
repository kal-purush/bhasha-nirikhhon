using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Video;
using UnityEngine.SceneManagement;

public class MovieFinish_Y : MonoBehaviour
{
    public VideoPlayer videoPlayer;
    public SceneReference nextScene;
    // Start is called before the first frame update
    void Start()
    {
        videoPlayer.loopPointReached += LoopPointReached;
    }

    public void LoopPointReached(VideoPlayer vp)
    {
        SceneManager.LoadScene(nextScene);
    }
}