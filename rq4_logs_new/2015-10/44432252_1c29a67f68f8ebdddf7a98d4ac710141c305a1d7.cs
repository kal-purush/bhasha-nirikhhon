using UnityEngine;
using System.Collections;

public class FogController : MonoBehaviour {

	public float timeToLive;
	public float opacityFactor;
	public Vector3 velocity;
	
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		if (timeToLive >= 0)
		{
			timeToLive -= Time.deltaTime;
			
			var oldColor = this.gameObject.GetComponent<SpriteRenderer>().material.color;
			var color = new Color(oldColor.r, oldColor.g, oldColor.b, oldColor.a - opacityFactor);
		}
		else
		{
			Destroy(gameObject);
		}
	}
	
	void FixedUpdate()
	{
		var targetPos = transform.position + (velocity * Time.deltaTime);
		transform.position = Vector3.Lerp(transform.position, targetPos, 0.25f);
	}
}