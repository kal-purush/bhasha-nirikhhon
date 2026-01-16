using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class NPCText : MonoBehaviour {

	public Text npcTextPrefab;

	// Update is called once per frame
	void Update () {

		Ray playerRay = new Ray (transform.position, transform.forward);
		RaycastHit hit = new RaycastHit();
		if (Physics.Raycast (playerRay, out hit, 10f)) {
			//npcTextPrefab.text = "LOL";
			Debug.DrawRay ( playerRay.origin, playerRay.direction * hit.distance, Color.green);
			if (hit.transform.gameObject.tag == "TV") {
				npcTextPrefab.text = "Need a sisscor to cut the wires";
			}
			else if (hit.transform.gameObject.tag == "General NPC") {
				npcTextPrefab.text = "Hello";
			}
			else if (hit.transform.gameObject.tag == "Pickupable") {
				npcTextPrefab.text = "Press E to pick up " + hit.transform.gameObject.name;
			}
		}
	}
}