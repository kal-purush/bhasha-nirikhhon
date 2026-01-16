using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TouchEmitter : MonoBehaviour {

    private Rigidbody rb;
    public bool strongWind = false;

    public float MoveSpeed = 50.0f;

    public float frequency = 1.0f;  // Speed of sine movement
    public float magnitude = 1.0f;   // Size of sine movement
    public Transform parentTrans;

    private Vector3 pos;

    // Use this for initialization
    void Start () {
       
       
        pos = transform.position;
        Destroy(gameObject, 1.0f);

    }
	
	// Update is called once per frame
	void Update () {

        pos += transform.TransformDirection(Vector3.forward) * Time.deltaTime * MoveSpeed;
        transform.position = pos;// + transform.up * Mathf.Sin(Time.time * frequency) * magnitude;
    }

    private void OnTriggerEnter(Collider other)
    {
        if (other.tag == "Panel"){
            other.gameObject.GetComponent<PanelSpin>().TouchSpinPanel();
        }
    }
}