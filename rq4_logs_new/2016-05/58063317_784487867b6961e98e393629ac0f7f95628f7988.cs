using System.Collections;
using UnityEngine;

class DraggableCloud : MonoBehaviour
{
	private Color unselected = new Color (1, 1, 1, 1f); 
	private Color selected = new Color (1, 1, 1, 0.5f);
	
	private bool dragging = false;
	private float distance;
	
	private Vector3 initPos;
	private bool goodPlace;
	
	private SpriteRenderer sr;
	
	void Start() {
		sr = GetComponent<SpriteRenderer> ();
		goodPlace = true;
	}
	
	void OnMouseEnter()
	{
		
	}
	
	void OnMouseExit()
	{
		
	}
	
	void OnMouseDown()
	{
		initPos = transform.position;
		distance = Vector3.Distance(transform.position, Camera.main.transform.position);
		dragging = true;
		sr.color = selected;
	}
	
	void OnMouseUp()
	{
		dragging = false;
		sr.color = unselected;
		if (goodPlace) {
			//this is okay
		} else {
			transform.position = initPos;
			goodPlace = true;
		}
	}
	
	void Update()
	{
		if (dragging)
		{
			Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
			Vector3 rayPoint = ray.GetPoint(distance);
			transform.position = new Vector3 (rayPoint.x, rayPoint.y, transform.position.z);
		}
	}
	
	void OnTriggerStay2D (Collider2D other) {
		goodPlace = false;
	}
	void OnTriggerExit2D (Collider2D other) {
		goodPlace = true;
	}
}