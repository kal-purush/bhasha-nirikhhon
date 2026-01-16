using UnityEngine;

public class SFXManager : MonoBehaviour
{
    public static SFXManager Instance {get;private set;}

    [SerializeField] private AudioClip damageSFX;
    
    private void Awake()
    {
        if (Instance == null)
        {
            Instance = this;
        }
    }

    public void PlaySFXClip(AudioClip clip, Transform spawnLocation, float volume)
    {
        // spawn game object
        
        // assign audio clip
        
        // assign volume
        
        // play sound
        
        // get length of sfx
        
        // destroy the clip after its done playing
    }

    public void PlayRandomSFXClip(AudioClip[] clip, Transform spawnLocation, float volume)
    {
        
    }
}