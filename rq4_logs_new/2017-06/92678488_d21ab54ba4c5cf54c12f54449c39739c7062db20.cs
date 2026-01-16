using UnityEngine;
using System.Collections;

public class BodyCore : MonoBehaviour 
{
	//==================================================

	public int landSection;
	public float landSteepness;
	public bool isGrounded;
	
	public GameObject obj;
	public LandCore land;

	//==================================================

	public void BodyInitialize()
	{
		obj = GameObject.Find("Land");
		land = obj.GetComponent<LandCore>();
		
		isGrounded = false;
		gameObject.GetComponent<Rigidbody2D>().gravityScale = 20;
	}

	//__________________________________________________

	public void CalculateLand()
	{
		// calculates current landSection and landSteepness
		
		int i;
		
		for (i=1; i<land.landSections; i++)
		{
			if (transform.position.x < land.landPointX[i])
			{
				if (i != landSection)
				{
					landSection = i;
					landSteepness = Mathf.Atan2(land.landPointY[i]-land.landPointY[i-1],land.landPointX[i]-land.landPointX[i-1]);
				}
				break;
			}
		}
	}

	//__________________________________________________

	public void PlaceOnGround()
	{
		float groundY = land.landPointY[landSection-1] + (transform.position.x-land.landPointX[landSection-1]) * Mathf.Tan(landSteepness);
		
		if (isGrounded == true)
		{
			transform.position = new Vector3 (transform.position.x, groundY);
		}
		else
			
		if (transform.position.y < groundY)
		{
			transform.position = new Vector3 (transform.position.x, groundY);
			gameObject.GetComponent<Rigidbody2D>().gravityScale = 0;
			isGrounded = true;
		}
	}

	//==================================================

}