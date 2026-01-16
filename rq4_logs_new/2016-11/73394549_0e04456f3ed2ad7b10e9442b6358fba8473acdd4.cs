using UnityEngine;
using System.Collections;

public class Location : MonoBehaviour
{
	public LocationType Type;
	public WayPoint LocationWaypoint;

	void Awake ()
	{
		GameManager.Instance.PossibleLocation.Add (this);
		if (LocationWaypoint == null)
			LocationWaypoint = GetComponent<WayPoint> ();
	}
}

public enum LocationType
{
	Shop = 0,
	BowlingAlley = 1,
	TennisCourt = 2
}