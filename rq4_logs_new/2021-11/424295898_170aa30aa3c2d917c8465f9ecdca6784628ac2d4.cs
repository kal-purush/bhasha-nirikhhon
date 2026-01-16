using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class InteractiveSoundController : MonoBehaviour
{

    [SerializeField] private AudioClip buttonClickSound;
    [SerializeField] private AudioClip levelSelectionSound;
    [SerializeField] private AudioClip levelCompletionSound;

    public void ButtonClickSound()
    {
        SoundManager.Instance.Play(buttonClickSound);
    }

    public void LevelSelectionSound()
    {
        SoundManager.Instance.Play(levelSelectionSound);
    }

    public void LevelFinishSound()
    {
        SoundManager.Instance.Play(levelCompletionSound);
    }
}