using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BridgeMoney : MonoBehaviour
{
    private bool _enable = false;
    private float _timer,_time;

    private void Start()
    {
        _timer = 1;
    }

    private void OnEnable()
    {
        _enable = true;
        _time = 0;
    }

    private void Update()
    {
        if (_enable)
        {
            _time += Time.deltaTime;
            if (_timer <= _time)
            {       
                _time = 0;
                _enable = false;

                this.gameObject.SetActive(false);
            }
        }
    }
}