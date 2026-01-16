using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PacmanManager : MonoBehaviour
{
    [SerializeField]
    private AudioSource PacmanMovementAudio;

    // Update is called once per frame
    void Update()
    {
        PacmanMovementAudio.Play();
    }
}