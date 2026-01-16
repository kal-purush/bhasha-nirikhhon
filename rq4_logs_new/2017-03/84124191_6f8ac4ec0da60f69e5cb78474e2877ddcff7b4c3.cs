using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Valve.VR;
using Valve.VR.InteractionSystem;

public class RackButtonScript : MonoBehaviour {
	[SerializeField]
	Transform pinprefab;

	[SerializeField]
	Transform pins;

	[SerializeField]
	Transform spawnpoint;



	// Use this for initialization
	void Start () {

	}

	// Update is called once per frame
	void Update () {

	}

	void HandHoverUpdate(Hand hand){
		if (hand.GetStandardInteractionButtonDown () == true){
			if (pins != null) {
				Destroy (pins.gameObject);
			}
			pins = Instantiate (pinprefab, spawnpoint.transform.position, Quaternion.identity);

		}
	}

}