using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LineOfSight : MonoBehaviour {
    private RaycastHit vision;
    public float raylentgh;
    private bool isgrabbed;
    private Rigidbody grabbedobject;
    private float force;
    private Camera fpsCam;
    // Use this for initialization
    void Start () {
        raylentgh = 4.0f;
        isgrabbed = false;
        fpsCam = GetComponentInParent<Camera>();
    }
	
	// Update is called once per frame
	void Update () {
        Debug.DrawRay(Camera.main.transform.position, Camera.main.transform.forward * raylentgh, Color.red, 0.5f);
        if(Physics.Raycast(Camera.main.transform.position, Camera.main.transform.forward, out vision, raylentgh))
        {
            if (vision.collider.tag == "hit")
            {
                Debug.Log(vision.collider.name);

                if (Input.GetKeyDown (KeyCode.Mouse0) && !isgrabbed)
                {
                    Vector3 push = fpsCam.ViewportToWorldPoint(new Vector3(0.5f, 0.5f, 0.0f));
                    vision.rigidbody.AddForce(push* 100);
                }
            }
        }
	}
}