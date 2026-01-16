using UnityEngine;

public class MainCharacterController : MonoBehaviour {
	[Range(0,1)][Tooltip("Percentage of the player's movement speed")]
	public float slowedSpeedModier = 0.2f;

	[Tooltip("Player's walk speed in Km/h")]
	public float playerWalkSpeed = 2.5f;
	[Tooltip("Player's run speed in Km/h")]
	public float playerRunSpeed = 3.5f;
	[Tooltip("Player's jump speed in number of Gs upward")]
	public float jumpSpeed = 13.0f;
	[Tooltip("Player's rotation speed in no specific unit")]
	public int rotateSpeed = 100;

	[Tooltip("Zoom step increment")]
	public float zoomIncrement = 20.0f;
	[Tooltip("Zoom maximum step")]
	public int maxZoom = 80;
	[Tooltip("Zoom minimum step")]
	public int minZoom = 30;

	[Tooltip("Mouse sensitivity for rotating the camera around.")]
	public float sensitivity = 1000f;

    [Tooltip("Distance from the ground to consider jumping.")]
    public float groundedThreshold = 0.3f;

    [HideInInspector]
    public Vector3 moveDirection = Vector3.zero;

    private GlobalGameController globalGameController;

    private float gravity = GlobalConstants.Gravity;

	private Camera cameraRef;
	private bool isFreeCam = false;

	private CharacterController playerController;
	private bool isGrounded = true;

	private float vertical_force;

	[HideInInspector]
	public bool isEncumbered { get; private set; }

	private float rot_x, rot_y = 0.0f;
	float distance = 0f;

	void Start () {
        globalGameController = GameObject.FindGameObjectWithTag(Tags.gameController).GetComponent<GlobalGameController>();

        cameraRef = globalGameController.playerCameraRef;

		isEncumbered = false;
		playerController = GetComponent<CharacterController>();

		FreeCamStartSetup();
    }

	void FixedUpdate () {
		float z = playerWalkSpeed * Input.GetAxis ("Horizontal") * Time.deltaTime * (-Vector3.left.x);
		float x = playerWalkSpeed * Input.GetAxis ("Vertical") * Time.deltaTime * (-Vector3.forward.z);

		// Recalculate movedirection directly from axes
		moveDirection = new Vector3(x, 0, z); //Determine the player's forward speed based upon the input.

		/*if (Input.GetMouseButton(0)) {
			isEncumbered = true;
		} else {
			isEncumbered = false;
		}*/

		//Encumbered movement
		float modifier = 1f;
		if (isEncumbered)
			modifier *= slowedSpeedModier;

		//Sprint movement
		if(Input.GetButton("Sprint")) {
			moveDirection *= playerRunSpeed * modifier;
		}
		else {
			moveDirection *= playerWalkSpeed * modifier;
		}
		
		// Apply jump and gravity
		if(!isGrounded)
		{
            if (vertical_force != 0)
                moveDirection.y -= (gravity * Time.deltaTime) - (vertical_force * Time.deltaTime);

            if (vertical_force > 0)
			{
				vertical_force -= gravity * Time.deltaTime;
			}
			if(vertical_force < 0)
			{
				vertical_force = 0;
			}
		}
		if(Input.GetButton("Jump") && transform.position.y < groundedThreshold)
		{
            vertical_force = jumpSpeed;
		}

		//Camera Rotation
		float rot = 0;
		if (Input.GetMouseButton (1)) {
			if (!isFreeCam) {
				FreeCamStartSetup ();
				isFreeCam = true;
			}

			rot_x += Input.GetAxis ("Mouse Y") * sensitivity * Time.deltaTime;
			rot_y += Input.GetAxis ("Mouse X") * sensitivity * Time.deltaTime;

			if (rot_x > 90f)
				rot_x = 90f;
			else if (rot_x < -90f)
				rot_x = -90f;

			Vector3 lookPos = cameraRef.transform.position - transform.position;
			float angle = Mathf.Atan2 (lookPos.x, lookPos.z) * Mathf.Rad2Deg;
			transform.rotation = Quaternion.Lerp(transform.rotation, Quaternion.AngleAxis (angle + 180, Vector3.up), Time.deltaTime*10);

			cameraRef.transform.position = transform.position + Quaternion.Euler (rot_x, rot_y, 0f) * (distance * -Vector3.back);
			cameraRef.transform.LookAt (transform.position, Vector3.up);
		} else {
			if (isFreeCam)
				isFreeCam = !isFreeCam;
			
			if (Input.GetKey (KeyCode.Q))
				rot = -rotateSpeed * Time.deltaTime;
			if (Input.GetKey (KeyCode.E))
				rot = rotateSpeed * Time.deltaTime;
		}

		//Camera Zoom
		float mouseWheel = -Input.GetAxis ("Mouse ScrollWheel");
		if (mouseWheel != 0)
			cameraRef.fieldOfView = Mathf.Clamp (cameraRef.fieldOfView + mouseWheel * zoomIncrement, minZoom, maxZoom);

		// Move the controller
		CollisionFlags flags = playerController.Move(moveDirection);
        transform.Rotate (new Vector3 (0, rot, 0));

		isGrounded = (flags == CollisionFlags.Below);
    }

	void FreeCamStartSetup(){
		distance = Vector3.Distance (cameraRef.transform.position, transform.position);

		rot_x = -cameraRef.transform.rotation.eulerAngles.x;
		rot_y = cameraRef.transform.rotation.eulerAngles.y + 180;

		cameraRef.transform.position = transform.position + Quaternion.Euler (rot_x, rot_y, 0f) * (distance * -Vector3.back);
		cameraRef.transform.LookAt (transform.position, Vector3.up);
	}
}