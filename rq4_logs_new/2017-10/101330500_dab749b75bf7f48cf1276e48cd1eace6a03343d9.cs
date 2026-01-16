using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AlternateControllerHandler : MonoBehaviour {
    public float threshold = 6f;

    // play a sound when caught
    private AudioSource caughtSound;

    // the ball that the user can grab
    private GameObject collidingObject;

    // the ball that the user is holding
    private GameObject objectInHand;

    // the controller
    private SteamVR_TrackedObject trackedObj;

    // broadcast an event every time a ball is grabbed
    public delegate void BallGrabbed();

    public static BallGrabbed OnBallGrab;

    // broadcast an event every time a ball is let go of
    public delegate void BallReleased();

    public static BallReleased OnBallRelease;

    private SteamVR_Controller.Device Controller
    {
        get { return SteamVR_Controller.Input((int)trackedObj.index); }
    }

    void Awake()
    {
        trackedObj = GetComponent<SteamVR_TrackedObject>();
        caughtSound = GetComponent<AudioSource>();
    }

    private void SetCollidingObject(Collider col)
    {
        if (collidingObject || !col.GetComponent<Rigidbody>())
        {
            return;
        }

        collidingObject = col.gameObject;
    }

    public void OnTriggerEnter(Collider other)
    {
        SetCollidingObject(other);
    }

    public void OnTriggerStay(Collider other)
    {
        SetCollidingObject(other);
    }

    public void OnTriggerExit(Collider other)
    {
        if (!collidingObject)
        {
            return;
        }

        collidingObject = null;
    }

    private void GrabObject()
    {
        caughtSound.Play();

        if (OnBallGrab != null)
        {
            OnBallGrab();
        }

        objectInHand = collidingObject;
        collidingObject = null;

        var joint = AddFixedJoint();
        joint.connectedBody = objectInHand.GetComponent<Rigidbody>();
    }

    private FixedJoint AddFixedJoint()
    {
        FixedJoint fx = gameObject.AddComponent<FixedJoint>();
        fx.breakForce = 20000;
        fx.breakTorque = 20000;
        return fx;
    }

    private void ReleaseObject()
    {
        if (OnBallRelease != null)
        {
            OnBallRelease();
        }

        if (GetComponent<FixedJoint>())
        {
            GetComponent<FixedJoint>().connectedBody = null;
            Destroy(GetComponent<FixedJoint>());

            objectInHand.GetComponent<Rigidbody>().velocity = Controller.velocity;
            objectInHand.GetComponent<Rigidbody>().angularVelocity = Controller.angularVelocity;
        }

        objectInHand = null;
    }

    // Update is called once per frame
    void Update()
    {
        // if colliding object
        if (collidingObject)
        {
            GrabObject();
        }

        // if velocity > certain amount release
        if (GetComponent<Rigidbody>().velocity.magnitude > 1f)
        {
            if (objectInHand)
            {
                ReleaseObject();
            }
        }
    }
}