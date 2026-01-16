using System;
using UnityEngine;
using UnityStandardAssets.CrossPlatformInput;


[RequireComponent(typeof (MyKeymapping))]
public class FreeLookCam : PivotBasedCameraRig
{
    // This script is designed to be placed on the root object of a camera rig,
    // comprising 3 gameobjects, each parented to the next:

    // 	Camera Rig
    // 		Pivot
    // 			Camera

	[SerializeField] private float moveSpeed = 1f;                      // How fast the rig will move to keep up with the target's position.
	[Range(0f, 10f)] [SerializeField] private float turnSpeed = 1.5f;   // How fast the rig will rotate from user input.
	[SerializeField] private float turnSmoothing = 0.1f;                // How much smoothing to apply to the turn input, to reduce mouse-turn jerkiness
	[SerializeField] private float tiltMax = 75f;                       // The maximum value of the x axis rotation of the pivot.
	[SerializeField] private float tiltMin = 45f;                       // The minimum value of the x axis rotation of the pivot.
	[SerializeField] private bool lockCursor = false;                   // Whether the cursor should be hidden and locked.
	[SerializeField] private bool verticalAutoReturn = false;           // set wether or not the vertical axis should auto return

	private float lookAngle;                    // The rig's y axis rotation.
	private float tiltAngle;   // The pivot's x axis rotation.
	private float tiltAngle2;
    private const float k_LookDistance = 100f;    // How far in front of the pivot the character's look target is.
	private Vector3 pivotEulers;
	private Quaternion pivotTargetRot;
	private Quaternion transformTargetRot;
	private MyKeymapping keymap;

    protected override void Awake()
    {
        base.Awake();
        // Lock or unlock the cursor.
        Cursor.lockState = this.lockCursor ? CursorLockMode.Locked : CursorLockMode.None;
        Cursor.visible = !this.lockCursor;
		this.pivotEulers = this.pivot.rotation.eulerAngles;

        this.pivotTargetRot = this.pivot.transform.localRotation;
		this.transformTargetRot = transform.localRotation;
		this.keymap = GetComponent<MyKeymapping> ();
    }


    protected void Update()
    {
        HandleRotationMovement();
        if (this.lockCursor && Input.GetMouseButtonUp(0))
        {
            Cursor.lockState = this.lockCursor ? CursorLockMode.Locked : CursorLockMode.None;
            Cursor.visible = !this.lockCursor;
        }
    }


    private void OnDisable()
    {
        Cursor.lockState = CursorLockMode.None;
        Cursor.visible = true;
    }


    protected override void FollowTarget(float deltaTime)
    {
        if (this.target == null) return;
        // Move the rig towards target position.
        transform.position = Vector3.Lerp(transform.position, this.target.position, deltaTime*this.moveSpeed);
    }


    private void HandleRotationMovement()
    {
		if(Time.timeScale < float.Epsilon)
		return;

        // Read the user input
        //var x = CrossPlatformInputManager.GetAxis("Mouse X");
        //var y = CrossPlatformInputManager.GetAxis("Mouse Y");

		var x = this.keymap.Camera_HorizontalAxis();
		var y = this.keymap.Camera_VerticalAxis();
        // Adjust the look angle by an amount proportional to the turn speed and horizontal input.
		this.lookAngle += x*this.turnSpeed;
		//m_TiltAngle2 += x * m_TurnSpeed;

        // Rotate the rig (the root object) around Y axis only:
        this.transformTargetRot = Quaternion.Euler(0f, this.lookAngle, 0f);
		//m_TransformTargetRot = Quaternion.Euler(m_TiltAngle,m_LookAngle,0f);
        if (this.verticalAutoReturn)
        {
            // For tilt input, we need to behave differently depending on whether we're using mouse or touch input:
            // on mobile, vertical input is directly mapped to tilt value, so it springs back automatically when the look input is released
            // we have to test whether above or below zero because we want to auto-return to zero even if min and max are not symmetrical.
            this.tiltAngle = y > 0 ? Mathf.Lerp(0, -this.tiltMin, y) : Mathf.Lerp(0, this.tiltMax, -y);
        }
        else
        {
            // on platforms with a mouse, we adjust the current angle based on Y mouse input and turn speed
            this.tiltAngle += y*this.turnSpeed;
            // and make sure the new value is within the tilt range
            this.tiltAngle = Mathf.Clamp(this.tiltAngle, -this.tiltMin, this.tiltMax);
        }

        // Tilt input around X is applied to the pivot (the child of this object)
		//this.pivotTargetRot = Quaternion.Euler(m_TiltAngle, this.pivotEulers.y , this.pivotEulers.z);

		this.pivotTargetRot = Quaternion.Euler(this.tiltAngle, this.pivotEulers.y , this.pivotEulers.z);
		if (this.turnSmoothing > 0)
		{
			this.pivot.localRotation = Quaternion.Slerp(this.pivot.localRotation, this.pivotTargetRot, this.turnSmoothing * Time.deltaTime);
			transform.localRotation = Quaternion.Slerp(transform.localRotation, this.transformTargetRot, this.turnSmoothing * Time.deltaTime);
		}
		else
		{
			this.pivot.localRotation = this.pivotTargetRot;
			transform.localRotation = this.transformTargetRot;
		}
    }
}
