using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(Rigidbody))]
public class Player_Movement : MonoBehaviour
{
    public bool isFist_L { get; private set; } = false;
    public bool isFist_R { get; private set; } = false;

    [Header("Hands")]
    [SerializeField] private GameObject leftHand;
    [SerializeField] private GameObject rightHand;

    [Header("Properties")]
    [SerializeField] bool TestMode;
    [SerializeField] private float forwardForce = 10f;
    [SerializeField] private float cooldownDuration = 1f;
    [SerializeField] private float velocityThreshold = 1f;
    [SerializeField] private float smoothingFactor = 0.1f;

    private Rigidbody playerRigidbody;
    private float lastForceTime = -Mathf.Infinity;
    private Vector3 previousLeftHandPos;
    private Vector3 previousRightHandPos;
    private Vector3 smoothedLeftHandVelocity;
    private Vector3 smoothedRightHandVelocity;

    private void Start()
    {
        playerRigidbody = GetComponent<Rigidbody>();

        //When start, previous position equal to init position of both hands.
        previousLeftHandPos = leftHand.transform.position;
        previousRightHandPos = rightHand.transform.position;
    }

    void FixedUpdate()
    {
        TestModeFunction();
        Player_Movement_Fun();
    }
    void Player_Movement_Fun()
    {
        if (Both_Hands_Fist())
        {
            Hands_Velocity_Process();

            // ApplyForce
            if (smoothedLeftHandVelocity.z < -velocityThreshold && smoothedRightHandVelocity.z < -velocityThreshold)
            {
                ApplyForwardForce();
            }
        }
    }
    private void Hands_Velocity_Process()
    {
        // Get velocity of both hands (v = dis/time)
        Vector3 leftHandVelocity = (leftHand.transform.position - previousLeftHandPos) / Time.fixedDeltaTime;
        Vector3 rightHandVelocity = (rightHand.transform.position - previousRightHandPos) / Time.fixedDeltaTime;

        // Smooth the velocity(Reduce errors)
        smoothedLeftHandVelocity = Vector3.Lerp(smoothedLeftHandVelocity, leftHandVelocity, smoothingFactor);
        smoothedRightHandVelocity = Vector3.Lerp(smoothedRightHandVelocity, rightHandVelocity, smoothingFactor);

        // Update previous pos of both hands.
        previousLeftHandPos = leftHand.transform.position;
        previousRightHandPos = rightHand.transform.position;
    }
    public void ApplyForwardForce()
    {
        if (Time.time - lastForceTime >= cooldownDuration)
        {
            // Normalized direction.
            Vector3 averageDirection = (smoothedLeftHandVelocity + smoothedRightHandVelocity).normalized;
            //Force direction.(expect y dir)
            Vector3 forceDirection = -new Vector3(averageDirection.x, 0, averageDirection.z);
            //Apply the force
            playerRigidbody.AddForce(forceDirection * forwardForce, ForceMode.Impulse);
            //Update lastForce time  
            lastForceTime = Time.time;
        }
    }

    public bool Both_Hands_Fist()
    {
        return isFist_L && isFist_R;
    }

    public void WhenFist_L()
    {
        isFist_L = true;
    }

    public void WhenNotFist_L()
    {
        isFist_L = false;
    }

    public void WhenFist_R()
    {
        isFist_R = true;
    }

    public void WhenNotFist_R()
    {
        isFist_R = false;
    }
    void TestModeFunction()
    {
        if (Input.GetKeyDown(KeyCode.Space) && TestMode)
        {
            ApplyForwardForce();
        }
    }
}