using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class Camera4 : MonoBehaviour
{
    public Transform _camerasFocusPoint;

    public float _yMin;
    public float _yMax;

    private VelocityBounce2 _playerBounce;
    private PlayerControl _playerControl;

    public bool _hitBounceBug;
    public bool _canDash;
    public bool _shouldTransition;

    private void Start()
    {
        _canDash = true;
        _playerBounce = FindObjectOfType<VelocityBounce2>();
        _playerControl = FindObjectOfType<PlayerControl>();
    }

    private void LateUpdate()
    {
        if (_playerBounce._hitHeight)
        {
            if (_shouldTransition)
            {
                _playerControl._forwardDashActivated = false;
                TransitionCameraWhenHitHeight();
            }
        }
        else
        {
            if (_hitBounceBug)
            {
                if (_shouldTransition)
                {
                    _playerControl._forwardDashActivated = false;
                    TransitionCameraHitBounceBug();
                }
            }
            else
            {
                _shouldTransition = true;
                if (_camerasFocusPoint.localPosition.y != 8.5f && !_playerControl._forwardDashActivated)
                {
                    ResetCameraWhenHitGround();
                }
            }
        }

        //camera follows target point
        transform.position = new Vector3(0, Mathf.Clamp(_camerasFocusPoint.position.y, _yMin, _yMax), transform.position.z);
    }

    private void TransitionCameraWhenHitHeight()
    {
        if (_camerasFocusPoint.localPosition.y < -8.5f)
        {
            _camerasFocusPoint.localPosition = new Vector3(0, -8.5f, transform.position.z);
        }
        else if (_camerasFocusPoint.localPosition.y != -8.5f)
        {
            _camerasFocusPoint.localPosition = new Vector3(0, _camerasFocusPoint.localPosition.y - .225f, transform.position.z);
        }

        if (_camerasFocusPoint.localPosition.y == -8.5f)
        {
            _shouldTransition = false;
        }
    }

    private void TransitionCameraHitBounceBug()
    {
        if (_camerasFocusPoint.localPosition.y > 8.5f)
        {
            _camerasFocusPoint.localPosition = new Vector3(0, 8.5f, transform.position.z);
        }
        else if (_camerasFocusPoint.localPosition.y != 8.5f)
        {
            _camerasFocusPoint.localPosition = new Vector3(0, _camerasFocusPoint.localPosition.y + .3f, transform.position.z);
        }

        if (_camerasFocusPoint.localPosition.y == 8.5f)
        {
            _shouldTransition = false;
            _hitBounceBug = false;
        }
    }

    private void ResetCameraWhenHitGround()
    {
        _camerasFocusPoint.localPosition = new Vector3(0, 8.5f, transform.position.z);
    }
}