using UnityEngine;
using System.Collections;

public class Door : MonoBehaviour {
	public bool isInside = false;

	void OnTriggerEnter2D(Collider2D col)
	{
		Debug.Log (col.gameObject.name);
		if (col.gameObject.tag == "Player") {
			if (!isInside) {
				this.GetComponent<SpriteRenderer> ().enabled = false;
				this.transform.FindChild ("Inside").gameObject.SetActive (true);
				isInside = true;
			} else {
				this.GetComponent<SpriteRenderer> ().enabled = true;
				this.transform.FindChild ("Inside").gameObject.SetActive (false);
				isInside = false;
			}
		}
	}
}