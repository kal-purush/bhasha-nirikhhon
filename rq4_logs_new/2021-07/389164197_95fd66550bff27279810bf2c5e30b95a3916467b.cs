using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioManager : MonoBehaviour
{
    [SerializeField] AudioClip walkingSound;
    private AudioSource audioSource;
    // Start is called before the first frame update
    void Start()
    {
        audioSource = this.gameObject.GetComponent<AudioSource>();
        EventBroadcaster.Instance.AddObserver(EventNames.ON_PLAYER_WALK, playWalkingSound);
        EventBroadcaster.Instance.AddObserver(EventNames.ON_PLAYER_WALK_STOP, stopWalkingSound);
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void OnDestroy()
    {
        EventBroadcaster.Instance.RemoveObserver(EventNames.ON_PLAYER_WALK);
        EventBroadcaster.Instance.RemoveObserver(EventNames.ON_PLAYER_WALK_STOP);
    }
    private void playWalkingSound()
    {
        audioSource.clip = walkingSound;
        if(!audioSource.isPlaying)
        audioSource.Play();
    }

    private void stopWalkingSound()
    {
        audioSource.Stop();
    }
}