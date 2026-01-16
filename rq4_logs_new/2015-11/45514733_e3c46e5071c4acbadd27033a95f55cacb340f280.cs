using UnityEngine;
using System.Collections;

public class MainController : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetMouseButton(0)) {
			Vector2 worldPoint = Camera.main.ScreenToWorldPoint(Input.mousePosition);
			RaycastHit2D hit = Physics2D.Raycast(worldPoint, Vector2.zero);
			if (hit.collider != null && hit.collider.gameObject.tag == "player") {
				//print ("hit collider");
				hit.collider.gameObject.SendMessageUpwards("Explosive", SendMessageOptions.DontRequireReceiver);
				//hit.collider.gameObject.SendMessage("Explosive", SendMessageOptions.DontRequireReceiver);

			}
		}
	}
}