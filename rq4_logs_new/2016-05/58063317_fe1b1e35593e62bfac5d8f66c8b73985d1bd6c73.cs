using System.Collections;
using UnityEngine;

class Draggable2 : MonoBehaviour
{

	private bool dragging = false;
	private float distance;
	
	private SpriteRenderer sr;

	void Start() {
		sr = GetComponent<SpriteRenderer> ();
	}
	
	void OnMouseEnter()
	{
		
	}
	
	void OnMouseExit()
	{
		
	}
	
	void OnMouseDown()
	{
		distance = Vector3.Distance(transform.position, Camera.main.transform.position);
		dragging = true;
	}
	
	void OnMouseUp()
	{
		dragging = false;
	}
	
	void Update()
	{
		if (dragging)
		{
			Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
			Vector3 rayPoint = ray.GetPoint(distance);
			transform.position = new Vector3 (rayPoint.x, rayPoint.y, 0f);
		}
	}

}