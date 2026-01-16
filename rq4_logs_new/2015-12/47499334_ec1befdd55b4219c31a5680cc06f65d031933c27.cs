using UnityEngine;
using System.Collections;

public class minigame_basketball_ball : MonoBehaviour {
	public GameObject arrow;

	private GameObject currentArrow;
	private Rigidbody2D rigidBody;
	private Camera camera;
//	void Awake() {
//	}
	// Use this for initialization
	void Start () {
		camera = GameObject.FindGameObjectWithTag("MainCamera").GetComponent<Camera>();

		rigidBody = GetComponent<Rigidbody2D> ();
		rigidBody.isKinematic = true;

	}
	
//	// Update is called once per frame
//	void Update () {
//
//	}
	void OnMouseDown() {
		print ("on mouse down");
		Vector2 spawnPoint = transform.position;
		currentArrow = (GameObject)Instantiate(arrow, spawnPoint, Quaternion.identity);
		print ("arrow position" + currentArrow.transform.position);
	}
	void OnMouseDrag() {
		Vector2 diff = diffVector ();
		if (currentArrow) {
			print ("onMouseDragging, diff" + diff);


			currentArrow.transform.rotation = Quaternion.FromToRotation (Vector2.right, diff);
			float scale = Mathf.Min (diff.magnitude, 2);
			currentArrow.transform.localScale = new Vector3 (scale, scale, 1);
		}
	}
	void OnMouseUp() {
		print ("on mouse up");
		Destroy (currentArrow);
		rigidBody.isKinematic = false;

		Vector2 diff = diffVector () * 500;
		rigidBody.AddForce (diff);
	}

	Vector2 diffVector() {
		Vector3 pos = camera.ScreenToWorldPoint(Input.mousePosition);;
		Vector2 diff = pos - transform.position;
		return diff;
	}
}