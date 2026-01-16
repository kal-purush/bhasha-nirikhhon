using UnityEngine;
using System.Collections;

[RequireComponent(typeof(CircleCollider2D))]
public class ClickAndDrag : MonoBehaviour {
	private Vector3 offset;
	public TrailRenderer trail;
	public GameObject StartingLocation;
	public bool MouseReleased = false;
	
	void Start () {
		trail.time = Mathf.Infinity;
	}
	
	void Update () {
	
	}
	
	//TODO Need to fix up so that is snaps to center of the object
	void OnMouseDown() {
		offset = gameObject.transform.position - Camera.main.ScreenToWorldPoint(new Vector3(Input.mousePosition.x, Input.mousePosition.y, 0));
		trail.time = Mathf.Infinity;
		MouseReleased = false;
	}
	
	void OnMouseDrag() {
		Vector3 curScreenPoint = new Vector3(Input.mousePosition.x, Input.mousePosition.y, 0);
		Vector3 curPosition = Camera.main.ScreenToWorldPoint(curScreenPoint) + offset;
		transform.position = curPosition;
		MouseReleased = false;
	}
	
	//TODO FADE trail renderer WHEN IT IS DESTROYED
	public void OnMouseUp() {
		//IF THIS DOES NOT COLLIDE WITH ENDGAME OBJECT if () {
		trail.time = 0;
		//}
		//IF IT DOES NOT COLLIDE WITH ENGAME OBJECT if() {
		this.gameObject.SetActive(false);
		this.gameObject.transform.position = StartingLocation.transform.position;
		this.gameObject.SetActive(true);
		MouseReleased = true;
		//}
	}
	
}