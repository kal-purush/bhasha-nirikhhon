using UnityEngine.Video;
using UnityEngine;

public class TransitionController : MonoBehaviour {
    VideoPlayer videoPlayer;

	// Start
	void Start () {
        videoPlayer = GetComponent<VideoPlayer>();
        videoPlayer.Play();		
	}
	
	// Update 
	void Update () {
        if(!videoPlayer.isPlaying)
        {
            Debug.Log("Stopped playing");
            GameController.LoadScene("Night1");            
        }		
	}
}