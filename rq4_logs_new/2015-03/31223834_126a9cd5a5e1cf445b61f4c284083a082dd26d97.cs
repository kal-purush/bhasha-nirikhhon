using UnityEngine;
using System.Collections;

public class KeyCard : MonoBehaviour{

	private CardboardHead head;
	private GameObject player;
	public GameObject keypadObj;
	private Keypad keypadScript;
	float distance = 10.0F;

	void Start () {
		player = GameObject.FindWithTag("MainCamera"); // Find the object tagged as MainCamera
		head = Camera.main.GetComponent<StereoController>().Head;
		keypadScript = keypadObj.GetComponent<Keypad> ();
	}
	
	void Update () {

		distance = Vector3.Distance (this.transform.position, player.transform.position);
		
		RaycastHit hit;
		bool isLookedAt = GetComponent<Collider>().Raycast(head.Gaze, out hit, Mathf.Infinity);

		if (isLookedAt && distance < 2) {
			GetComponent<Renderer> ().material.color = Color.green;
			
			if (Cardboard.SDK.CardboardTriggered) {
				keypadScript.gotKey();
				Destroy(gameObject);
			}
		} if (!isLookedAt){
			GetComponent<Renderer> ().material.color = Color.white;
		}
	}
}