using UnityEngine;
using System.Collections;

public class maprotator : MonoBehaviour {
	public GameObject planet;
	float sDistance;
	float pDistance;
	public float ratio;

	// Use this for initialization
	void Start () {
		sDistance = Vector3.Magnitude (this.transform.localPosition);
		pDistance = Vector3.Magnitude (planet.transform.localPosition);
		ratio = sDistance / pDistance;
	}
	
	// Update is called once per frame
	void Update () {
		this.transform.localPosition = new Vector3 (planet.transform.localPosition.z * ratio, planet.transform.localPosition.x * ratio, 0);
	}
}