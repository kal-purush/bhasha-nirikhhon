using System.Collections;
using UnityEngine;

class TenTonDrag : MonoBehaviour
{
	public GameObject deadly;
	private Dangerous d;

	private Color unselected = new Color (1, 1, 1, 1f); 
	private Color selected = new Color (1, 1, 1, 0.5f);
	
	private bool dragging = false;
	private float distance;
	
	private Vector3 initPos;
	private bool goodPlace;
	
	private SpriteRenderer sr;
	private Collider2D[] colliders;
	
	void Start() {
		d = deadly.GetComponent<Dangerous> ();
		sr = GetComponent<SpriteRenderer> ();
		colliders = GetComponents<Collider2D> ();
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
		foreach (Collider2D c in colliders) {
			c.isTrigger = true;
		}
	}
	
	void OnMouseUp()
	{
		dragging = false;
		sr.color = unselected;
		foreach (Collider2D c in colliders) {
			c.isTrigger = false;
		}
		if (goodPlace) {
			//this is okay
		} else {
			transform.position = initPos;
			goodPlace = true;
		}
	}
	
	void Update()
	{
		d.active = !dragging;
		if (dragging)
		{
			Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
			Vector3 rayPoint = ray.GetPoint(distance);
			transform.position = new Vector3 (rayPoint.x, rayPoint.y, 0f);
		}
	}
	
	void OnTriggerStay2D (Collider2D other) {
		goodPlace = false;
	}
	void OnTriggerExit2D (Collider2D other) {
		goodPlace = true;
	}
}