using System.Collections;
using System.Collections.Generic;
using UnityEngine;

// Thank you @James for the base script
public class Parallax : MonoBehaviour
{

    [Header("Parallax Parameters")]
    public Vector2 factor;
    private Vector3 _startPos;

    // [Header("Scrolling")]
    // float scrollSpeedX = 0;

    private Vector3 _centerOfParallax;
    private Camera _mainCamera;
    private Vector2 _deltaPos;

    void Awake()
    {
        _mainCamera = Camera.main;
    }

    void Start()
    {
        _startPos = transform.position;
        _centerOfParallax = Vector3.zero;
    }

    void Update()
    {
        if (_centerOfParallax == null || _mainCamera == null) return;

        _deltaPos = _mainCamera.transform.position - _centerOfParallax;
        _deltaPos *= factor;
        this.transform.position = _startPos + (Vector3)_deltaPos;
    }
}