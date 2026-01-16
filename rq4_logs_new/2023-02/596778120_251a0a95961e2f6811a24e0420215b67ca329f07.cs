using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Windows;

public class ScriptEventListiner : MonoBehaviour
{
    [SerializeField]
    private float _time = 0f;
    [SerializeField]
    private float _resetTime = 3f;
    private bool _interacted = false;

    private MeshRenderer _meshRend;
    void Start()
    {
        _meshRend = GetComponent<MeshRenderer>();
        InputManager.OnInteraction += SetTime;
    }
    void Update()
    {
        if (_time + _resetTime < Time.time)
        {
            _time = Time.time;
            _interacted = false;
            _meshRend.material.color = Color.white;
        }
    }
    private void SetTime()
    {
        _meshRend.material.color = Color.red;
        _interacted = true;
        _time = Time.time;
    }
}