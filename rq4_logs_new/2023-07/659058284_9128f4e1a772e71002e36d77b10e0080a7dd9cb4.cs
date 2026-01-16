using UnityEngine;
using UnityEngine.UI;

public class ButtonSoundPlayer : MonoBehaviour
{
    public Button yourButton; // Reference to your button in the scene
    public AudioSource audioSource; // Reference to the AudioSource component

    private void Start()
    {
        // Register a listener for the button's onClick event
        yourButton.onClick.AddListener(PlaySoundOnClick);
    }

    private void PlaySoundOnClick()
    {
        // Play the sound when the button is clicked
        if (audioSource != null && !audioSource.isPlaying)
        {
            audioSource.Play();
        }
    }
}
