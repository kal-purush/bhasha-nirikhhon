using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Check : MonoBehaviour
{
    [SerializeField] private Chronometer chronometer;

    void Update()
    {
        if (chronometer.value <= 0 && chronometer.toggle)
        {
            Application.LoadLevel(Application.loadedLevel);
        }
    }
}