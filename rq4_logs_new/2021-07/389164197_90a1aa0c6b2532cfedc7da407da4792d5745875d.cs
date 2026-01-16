using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BGMManager : MonoBehaviour
{
    [SerializeField] AudioClip BGM;

    [SerializeField] private AudioSource audioSourceBGM;


    // Start is called before the first frame update
    void Start()
    {
        audioSourceBGM = this.gameObject.GetComponent<AudioSource>();
        EventBroadcaster.Instance.AddObserver(EventNames.ON_PLAY_BGM, playBGM);
        EventBroadcaster.Instance.AddObserver(EventNames.ON_STOP_ALL_BGM, stopBGM);
    }

    private void OnDestroy()
    {
        EventBroadcaster.Instance.RemoveObserver(EventNames.ON_PLAY_BGM);
        EventBroadcaster.Instance.RemoveObserver(EventNames.ON_STOP_ALL_BGM);
    }

    // Update is called once per frame
    void Update()
    {
        if (audioSourceBGM.isPlaying)
        {
            Debug.Log("playing");
        }
    }

    private void playBGM()
    {
        audioSourceBGM.clip = BGM;
        audioSourceBGM.Play();
        Debug.Log("played func");

    }

    private void stopBGM()
    {
        if (audioSourceBGM.isPlaying)
        {
            audioSourceBGM.Stop();
        };
    }
}