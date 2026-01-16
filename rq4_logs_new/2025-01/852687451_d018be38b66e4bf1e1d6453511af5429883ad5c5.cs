using UnityEngine;
using UnityEngine.UI;

[RequireComponent(typeof(AudioSource), typeof(Button))]
public class ButtonAudio : MonoBehaviour
{
    [SerializeField] private AudioSource _audioSource;

    #region Validate
    private void OnValidate()
    {
        _audioSource = GetComponent<AudioSource>();
        _audioSource.playOnAwake = false;
        GetComponent<Button>().onClick.AddListener(PlayClickSound);
    }
    #endregion

    public void PlayClickSound()
    {
        _audioSource.Play();
    }
}