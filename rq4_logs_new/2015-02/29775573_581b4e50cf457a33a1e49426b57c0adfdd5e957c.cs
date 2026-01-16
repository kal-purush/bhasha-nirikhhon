using UnityEngine;
using System.Collections;

public class Camera : MonoBehaviour {

    public float leftMargin;
    public float rightMargin;

    private Vector3 endMarker;
    
    // Use this for initialization
	void Start () {
        endMarker = transform.position;
    }

    private Vector3 velocity = Vector3.zero;

	// Update is called once per frame
	void Update () {
        transform.position = Vector3.SmoothDamp(transform.position, endMarker, ref velocity, 1.25f);
    }

    void OnTriggerEnter(Collider other)
    {
        if(other.transform.localScale.x < 0)  // going to the right
        {
            endMarker = new Vector3(rightMargin, transform.position.y, transform.position.z);
        }
        else
        {
            endMarker = new Vector3(leftMargin, transform.position.y, transform.position.z);
        }
    }

    void OnTriggerExit(Collider other)
    {
        if (other.transform.localScale.x < 0)  // going to the right
        {
            endMarker = new Vector3(rightMargin, transform.position.y, transform.position.z);
        }
        else
        {
            endMarker = new Vector3(leftMargin, transform.position.y, transform.position.z);
        }
    }

}
