using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class CamSwitch : MonoBehaviour {
	public List<GameObject> characters = new List<GameObject> (4);
	int activeCharacter = 0;
	void LateUpdate () {
		if (Input.GetKeyUp (KeyCode.UpArrow)) {
			activeCharacter = 0;
		} else if (Input.GetKeyUp (KeyCode.LeftArrow)) {
			activeCharacter = 1;
		} else if (Input.GetKeyUp (KeyCode.DownArrow)) {
			activeCharacter = 2;
		} else if (Input.GetKeyUp (KeyCode.RightArrow)) {
			activeCharacter = 3;
		}
		Vector3 goal = characters[activeCharacter].transform.position + Vector3.forward*-10;
		transform.position = Vector3.Lerp (transform.position,goal,0.05f);
    }
}