using UnityEngine;
using System.Collections;

public class ButtonMovementAndMotion : MonoBehaviour {
	public Transform target;
	public float speed;
	
	void Update () {
		
	}
	
	public void MoveRight () {
		float step = speed * Time.deltaTime;
		Vector3 currentLocation = this.transform.position;
		
		transform.position = Vector3.MoveTowards(currentLocation, target.position, step);
	}
	
	public void MoveLeft () {
	
	}
	
	public void MoveUp () {
	
	}
	
	public void MoveDown () {
	
	}
	
	public void Go () {
	
	}
	
	public void CheckingForObject() {
		//GameObject nextClosestObject = gameObject.Find("PuzzlePoint");
	}
}