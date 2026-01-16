using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PanelSpin : MonoBehaviour {

    private Rigidbody _rb;
    [SerializeField] private float _rotSpeed=100f;
	// Use this for initialization
	void Start () {
        _rb = GetComponent<Rigidbody>();
        //_rb.AddTorque(Vector3.forward * _rotSpeed);

    }
	
	// Update is called once per frame
	void Update () {

    }

    public void SpinPanel(){
        _rb.AddTorque(Vector3.forward * _rotSpeed);

    }
}