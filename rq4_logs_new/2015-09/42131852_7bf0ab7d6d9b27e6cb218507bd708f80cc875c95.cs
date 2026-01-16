using UnityEngine;
using System.Collections;

public class PlayerAnimations : MonoBehaviour {

	private string currentDirection = "right";
	
	// Update is called once per frame
	void Update () {
		
		if (Input.GetKey ("right")) {
			this.changeDirection ("right");
		}
		if (Input.GetKey ("left")) {
			this.changeDirection ("left");
		}
	}
	//--------------------------------------
	// Flip player sprite for left/right walking
	//--------------------------------------
	void changeDirection(string direction)
	{
		Vector3 targetRotation = this.transform.rotation.eulerAngles;
		if (currentDirection != direction)
		{
			if (direction == "right")
			{
				targetRotation.Set(0,0,0);
				currentDirection = "right";
			}
			else if (direction == "left")
			{
				targetRotation.Set(0,180,0);
				currentDirection = "left";
			}

			transform.rotation = Quaternion.Lerp(transform.rotation, Quaternion.Euler(targetRotation), Time.fixedDeltaTime * 100);
		}
	}

}