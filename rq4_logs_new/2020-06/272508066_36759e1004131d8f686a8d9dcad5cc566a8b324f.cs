using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Random = UnityEngine.Random;

public class NavyBrig : MonoBehaviour
{
    public float AngleVelocity = 1f;
    public float PassCourseTiming = 5;
    public float YawAngle = 15;
    public float EngineMaxRpm = 600;
    public float AccelerationStep = 20;
    public float MaxAngleShift = 60;

    private Rigidbody _rb;
    private float _nextAdjust = 0;
    private Animator _animator;
    private PropellerBoats _pb;
    private float _angle = 0;
    private float _engineRpm = 0;
    private float _throttle = 0;

    // Start is called before the first frame update
    void Start()
    {
        _animator = GetComponent<Animator>();

//        if (!gameObject.GetComponent<Rigidbody>())
//        {
//           _rb = gameObject.AddComponent<Rigidbody>();
//           _rb.velocity = transform.forward * Velocity;
//           _rb.useGravity = false;
//        }

        _rb = GetComponent<Rigidbody>();
        _pb = GetComponent<PropellerBoats>();
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKey(KeyCode.A))
            RudderLeft();
        else if (Input.GetKey(KeyCode.D))
            RudderRight();

        if (Input.GetKey(KeyCode.W)) {
            ThrottleUp();
        } else if (Input.GetKey(KeyCode.S)) {
            ThrottleDown();
        } 
        
//        if (!Input.GetKey(KeyCode.W) && !Input.GetKey(KeyCode.S)) {
//            Brake();
//        }

        ThrottleUp();
        AddTorque();

        if (Time.time > _nextAdjust)
        {
            for (int i = 0; i < MaxAngleShift; i++)
            {
                RudderRight();
            }

//            transform.Rotate(new Vector3(0,  YawAngle, 0));
//        
//            _rb.velocity = transform.forward * Velocity;
            _nextAdjust = Time.time + Random.value * PassCourseTiming;
            
        }

        AngleDamping();
    }

    protected void AddTorque()
    {
        _engineRpm = _throttle * EngineMaxRpm;
        _rb.AddForceAtPosition(Quaternion.Euler(0, _angle, 0) * transform.forward * _engineRpm, transform.position);
    }
    public void ThrottleUp()
    {
        _throttle += AccelerationStep * 0.001F;
        _throttle = Mathf.Clamp(_throttle, 0f, 1f);
    }

    public void ThrottleDown()
    {
        _throttle -= AccelerationStep * 0.001F;
        _throttle = Mathf.Clamp(_throttle, -1, 0);
    }

    public void Brake()
    {
        if (_throttle > 0)
        {
            _throttle -= AccelerationStep * 0.001F;
        }
        else
        {
            _throttle += AccelerationStep * 0.001F;
        }
    }

    public void RudderRight()
    {
        _angle -= AngleVelocity;
        _angle = Mathf.Clamp(_angle, -1f * YawAngle, YawAngle);
    }

    public void RudderLeft()
    {
        _angle += AngleVelocity;
        _angle = Mathf.Clamp(_angle, -1f * YawAngle, YawAngle);
    }

    public void AngleDamping()
    {
        _angle = Mathf.Lerp(_angle, 0.0F, 0.02F);
    }


    private void OnTriggerEnter() 
    {
        Debug.Log(Math.Round(Random.value * 50));
        if (Math.Round(Random.value * 50) == 5)
            _animator.enabled = true;
    }
}