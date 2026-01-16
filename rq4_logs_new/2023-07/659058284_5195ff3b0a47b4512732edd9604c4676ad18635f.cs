using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.Video;

public class IntroTrans : MonoBehaviour
{
    // Reference to the VideoPlayer component
    public VideoPlayer videoPlayer;

    // Name of the scene to load after the video is finished playing
    public string sceneToLoad;

    void Start()
    {
        
        videoPlayer.loopPointReached += OnVideoFinished;
    }

    void OnVideoFinished(VideoPlayer vp)
    {
        // Load the specified scene after the video has finished playing
        SceneManager.LoadScene(sceneToLoad);
    }
}
