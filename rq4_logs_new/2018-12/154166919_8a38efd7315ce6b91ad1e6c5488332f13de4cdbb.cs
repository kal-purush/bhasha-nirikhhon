using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DisapearAndSignal : MonoBehaviour {
	public GameObject[] UnlockItems = new GameObject[5];
	public float DisplayTime;

	// Use this for initialization
	IEnumerator WaitDisapear() {
		yield return new WaitForSeconds(DisplayTime);
		this.GetComponent<SpriteRenderer>().sprite = null;
		foreach(GameObject item in UnlockItems){
			if(item) item.SendMessage("Unlocked");
		}
		Destroy(this.gameObject);
	}
	// Use this for initialization
	void Start () {
		StartCoroutine("WaitDisapear");
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}