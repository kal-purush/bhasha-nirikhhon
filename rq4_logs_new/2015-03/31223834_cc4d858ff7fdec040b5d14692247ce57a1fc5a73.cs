using UnityEngine;
using System.Collections;

public class QuitGame : MonoBehaviour {
	
	private CardboardHead head;
	private Vector3 startingPosition;
	private float delay = 0.0f; 
	public Color greyBlue = new Color(0.2F, 0.3F, 0.4F, 0.5F);
	
	void Start() {
		head = Camera.main.GetComponent<StereoController>().Head;
		startingPosition = transform.localPosition;
	}
	
	void Update() {
		RaycastHit hit;
		bool isLookedAt = GetComponent<Collider>().Raycast(head.Gaze, out hit, Mathf.Infinity);
		if (isLookedAt && Time.time>delay  && Cardboard.SDK.CardboardTriggered) { 
			Application.Quit();
			delay = Time.time + 2.0f;
		}
		// currently looking at object
		else if (isLookedAt) { 
			GetComponent<Renderer>().material.color = greyBlue; 
		} 
		// not looking at object
		else if (!isLookedAt) { 
			GetComponent<Renderer>().material.color = Color.white; 
			delay = Time.time + 2.0f; 
		}
	}
	
}