using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RotateEventArgs : EventArgs
{
    private Touch[] _trackedFingers;
    private ERotateDirection _direction;
    private float _angle;
    private GameObject _hitObject;

    public Touch[] TrackFingers
    {
        get { return this._trackedFingers; }
        set { this._trackedFingers = value; }
    }

    public ERotateDirection Direction
    {
        get { return this._direction; }
        set { this._direction = value; }
    }

    public float Angle
    {
        get { return this._angle; }
        set { this._angle = value; }
    }
    public GameObject HitObject
    {
        get { return this._hitObject; }
        set { this._hitObject = value; }
    }

    public RotateEventArgs(Touch[] trackedFingers, ERotateDirection direction, float angle, GameObject hitObject = null)
    {
        this._trackedFingers = trackedFingers;
        this._direction = direction;
        this._angle = angle;
        this._hitObject = hitObject;

    }


}