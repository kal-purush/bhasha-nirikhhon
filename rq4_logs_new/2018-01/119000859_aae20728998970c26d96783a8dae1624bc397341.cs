
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(ShooterProjectile))]
public class EngineNoise : MonoBehaviour
{


    private AudioSource movingAudio;
    private AudioSource stationaryAudio;

    void Awake()
    {
        AudioSource[] sources = GetComponents<AudioSource>();
        movingAudio = sources[0];
        stationaryAudio = sources[1];
      
    }

    // Update is called once per frame
    void Update() {
        if (Input.GetButton("Horizontal") || Input.GetButton("Vertical")) {
            movingAudio.volume = 0.9f;
            stationaryAudio.volume = 0.0f;
        }
        else
        {
            stationaryAudio.volume = 0.6f;
            movingAudio.volume = 0.0f;
        }

      
    }
}