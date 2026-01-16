using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ButtonSound : MonoBehaviour
{
    public AudioSource buttonClickSound; // Referensi ke komponen AudioSource

    // Method untuk memutar suara
    public void PlaySound()
    {
        if (buttonClickSound != null)
        {
            buttonClickSound.Play();
        }
    }
}
