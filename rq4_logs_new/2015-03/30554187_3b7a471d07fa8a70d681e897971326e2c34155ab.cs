using UnityEngine;
using System.Collections;

public class MoveAgent : MonoBehaviour {
	
	public float turnSpeed;
	public float maxShipSpeed;
	public float curShipSpeed;
	
	
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		
		//print(transform.up);
		transform.Rotate(Vector3.forward * -Input.GetAxis("Horizontal") * turnSpeed * Time.deltaTime);

		transform.position += transform.up * curShipSpeed * Time.deltaTime;
		
		curShipSpeed += Input.GetAxis("Vertical") * Time.deltaTime;
		
		curShipSpeed = Mathf.Clamp(curShipSpeed, 0.5f, maxShipSpeed);

	}
	
	void OnTriggerEnter(Collider hit)
	{
		print("hit a trigger");
		
	}
}