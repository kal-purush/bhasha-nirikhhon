using UnityEngine;
using System.Collections;

public class Platform : MonoBehaviour 
{
	#region Fields
	private bool hasLandedOn;
	public bool isStartingPlatform;
	#endregion

	#region Properties
	public bool HasLandedOn { get { return hasLandedOn; } }
	#endregion

	void Awake()
	{
		hasLandedOn = isStartingPlatform;
	}

	void OnCollisionEnter(Collision col)
	{
		if (col.gameObject.GetComponent<Player>() != null)// && col.gameObject.GetComponent<Player>().Body.velocity.y > 0f)
		{
			//Player landed so call the landing
			GameManager.Instance.LandedOnNewPlatform(this);
			hasLandedOn = true;
		}
	}
}