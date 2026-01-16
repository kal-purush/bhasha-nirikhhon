using UnityEngine;
using System.Collections;


// Written By Abdalla Tawfik & Fehr Hassan


public class CameraController : MonoBehaviour {


	#region Variables Declaration & Initialization

	// A reference to the Player Game Object.
	public GameObject player;

	// The distance between Camera position and the Player Position.
	private Vector3 offset;
	// The magnitude of the limit of Camera X position.
	private float xPosLimit = 1.5f;

	#endregion



	#region Unity Callbacks

	void Start ()
	{
		// The distance between Camera position and the Player Position. 
		offset = transform.position - player.transform.position;
	}
	
	void LateUpdate ()
	{
		// LateUpdate is called after all Update functions have been called.
		// A follow camera should always be implemented in LateUpdate because:
		// It tracks objects (Player) that might have moved inside Update.


		
		// Make the Camera follows the Player position on X and Z Axis.
		// And clamp the Camera's X position between - xPosLimit and xPosLimit.
		if (player != null)
			transform.position = new Vector3 (Mathf.Clamp(player.transform.position.x + offset.x, -xPosLimit, xPosLimit), transform.position.y, player.transform.position.z + offset.z);
		
	}

	#endregion
}