using UnityEngine;
using System.Collections;

public class Movement : MonoBehaviour
{


    public Transform ship;
    public Camera followCam;

    public float thrustMod;
    public float turnMod;


    private Vector3 camPosOffset;
    private Quaternion camRotOffset;

    private Rigidbody rb;

    // Use this for initialization
    void Start ()
    {
        camPosOffset = followCam.GetComponent<Transform>().position - ship.position;
        camRotOffset = followCam.GetComponent<Transform>().rotation;

        rb = ship.GetComponent<Rigidbody>();
    }
	
	// Update is called once per frame
	void Update ()
    {
        float thrust = Input.GetAxis("Vertical");
        float turn = Input.GetAxis("Horizontal");

        rb.AddForce(ship.up * thrustMod * thrust, ForceMode.Force);

        rb.AddTorque(ship.forward * -1 * turnMod * turn);


	}

    void LateUpdate()
    {
        followCam.transform.position = ship.position + camPosOffset;

        followCam.transform.rotation = camRotOffset;

        //followCam.transform.rotation = Quaternion.Euler(
        //    camRotOffset.eulerAngles.x + ship.rotation.eulerAngles.x, 
        //    camRotOffset.eulerAngles.y - ship.rotation.eulerAngles.y, 
        //    camRotOffset.eulerAngles.z - ship.rotation.eulerAngles.z
        //    );
        
        //followCam.transform.rotation = ship.rotation * Quaternion.Inverse(camRotOffset);
    }

}