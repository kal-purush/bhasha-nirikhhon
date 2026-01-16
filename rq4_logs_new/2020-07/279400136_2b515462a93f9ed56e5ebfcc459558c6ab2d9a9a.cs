using System.Collections;
using System.Collections.Generic;
using UnityEngine;


public class Sounds : MonoBehaviour
{
    public double delay;
    private double nextEventTime;

    void Start() {
        nextEventTime = AudioSettings.dspTime;
    }

    void Update() {
        double time = AudioSettings.dspTime;
        if (time + 1.0f > nextEventTime) {
            GetComponent<AudioSource>().PlayScheduled(nextEventTime);
            double rand = Random.Range(0,5);
            nextEventTime += delay + rand;
        }
    }
}