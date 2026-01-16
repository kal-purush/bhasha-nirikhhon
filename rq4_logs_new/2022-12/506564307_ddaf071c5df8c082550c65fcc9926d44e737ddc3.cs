using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FishingBoat : MonoBehaviour
{
    [SerializeField]
    private float _SizeOfShipSwaying;
    [SerializeField]
    private float _periodOfShipSwaying;

    private float _time = 0.0f;
    private float _xAngle;
    private float _zAngle;
    private Vector3 _initEulerAngles;

    // Start is called before the first frame update
    void Start()
    {
        _initEulerAngles = this.transform.eulerAngles;
    }

    // Update is called once per frame
    void Update()
    {
        _time += Time.deltaTime;
        _xAngle = _SizeOfShipSwaying * Mathf.Sin(2.0f * Mathf.PI * _time / _periodOfShipSwaying);
        _zAngle = _SizeOfShipSwaying * Mathf.Sin(2.0f * Mathf.PI * _time / _periodOfShipSwaying + 90.0f);
        this.transform.eulerAngles = _initEulerAngles + new Vector3(_xAngle, 0.0f, _zAngle);
    }
}