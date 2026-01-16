using System;
using System.Collections;
using System.Collections.Generic;
using _Scripts.Utils;
using UnityEngine;
using UnityEngine.VFX;

public class ConfettiLauncher : MonoBehaviour
{
    [SerializeField] private List<VisualEffect> _confettiLaunchers = new();

    [SerializeField] private float _loopInterval = 2.5f;

    private int _loopingTimerId = -1;
    
    public void Launch()
    {
        Timer.RemoveTimer(_loopingTimerId);
        _loopingTimerId = Timer.StartLoopingTimer(_loopInterval, () =>
        {
            foreach (VisualEffect launcher in _confettiLaunchers)
            {
                launcher.Play();
            }
        });
    }

    public void Stop()
    {
        Timer.RemoveTimer(_loopingTimerId);
    }
}