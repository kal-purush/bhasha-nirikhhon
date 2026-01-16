using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerAnimSound : MonoBehaviour
{
    public void PlaySound(string soundName)
    {
        AudioManager.instance.Play(soundName);
    }
}