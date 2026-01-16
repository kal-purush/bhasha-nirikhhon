using System.Collections;
using UnityEngine;

public class AlarmSoundContoller : MonoBehaviour
{
    private AudioSource _alarmSound;
    private float _fadeTime = 2f;
    private Coroutine changeSound;

    void Start()
    {
        _alarmSound = GetComponent<AudioSource>();
    }

    private IEnumerator StartFade(float targetVolume)
    {
        _alarmSound = GetComponent<AudioSource>();
        float startVolume = _alarmSound.volume;
        float elapsedTime = 0f;

        _alarmSound.Play();

        while (elapsedTime < _fadeTime)
        {
            elapsedTime += Time.deltaTime;
            _alarmSound.volume = Mathf.MoveTowards(startVolume, targetVolume, elapsedTime / _fadeTime);
            yield return null;
        }
    }

    public void PlaySound(float strengthSound)
    {
        if(changeSound != null)
        {
            StopCoroutine(changeSound);
        }

        changeSound = StartCoroutine(StartFade(strengthSound));
    }
}