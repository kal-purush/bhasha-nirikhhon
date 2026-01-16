using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.Video;

public class PlayIntro : MonoBehaviour
{
    public VideoPlayer videoPlayer;

    void Start()
    {
        videoPlayer.Play();
    }

    private void LateUpdate()
    {
        long playerCurrentFrame = videoPlayer.GetComponent<VideoPlayer>().frame;
        long playerFrameCount = System.Convert.ToInt64(videoPlayer.GetComponent<VideoPlayer>().frameCount);

        Debug.Log("CURRENT FRAME " + playerCurrentFrame);
        Debug.Log("FRAME COUNT " + playerFrameCount);

        if (playerCurrentFrame >= playerFrameCount - 1)
        {
            SceneManager.LoadScene("MasterScene");
        }
    }

}