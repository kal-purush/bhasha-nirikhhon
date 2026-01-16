using UnityEngine;
using System.Collections;

public class GateBehaviour : MonoBehaviour {

	public GameObject gate;
	public GameObject player;


	public float speed;
	private float min;
	private float max;
	private float currentY;

	private bool open = false;

	// Use this for initialization
	void Start () {
		min = 0.0f;
		max = 0.7f;
		currentY = gate.transform.localPosition.y;
	}
	
	// Update is called once per frame
	void Update () {
		currentY = gate.transform.localPosition.y;

		if (open) {
			if (gate.transform.localPosition.y <= max)
				currentY += speed;
		}


		else {
			if (gate.transform.localPosition.y >= min)
				currentY -= speed;
		}

		gate.transform.localPosition = new Vector3(gate.transform.localPosition.x, currentY, gate.transform.localPosition.z);

		Debug.Log (currentY);
	}

	void OnTriggerEnter2D(Collider2D other)
	{
		if (other.tag == "Player") {
			Debug.Log("Collides");
			open = true;
		}
	}

	void OnTriggerExit2D(Collider2D other)
	{
		if (other.tag == "Player") {
			Debug.Log("Out");
			open = false;
		}
	}
}