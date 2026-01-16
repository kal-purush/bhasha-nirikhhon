using UnityEngine;
using System.Collections;

public class ExitPoint : MonoBehaviour {

	//time from level start when exit will opens(in seconds)
	public float timeWhenOpens;

	//enabled(you can exit if you want)
	public bool exitOpened = false;


	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	void OnTriggerStay2D(Collider2D enteringCollider){
		if (enteringCollider.tag == "Player"){
			EndLevel();
		}
	}

	void EndLevel(){
		foreach (var item in GameObject.FindObjectsOfType<GameObject>()) {
			if (item.GetComponent<Camera> () == null) {
				Destroy (item);
			}
		}
	}
}