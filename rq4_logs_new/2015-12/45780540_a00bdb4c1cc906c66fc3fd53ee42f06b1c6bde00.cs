using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class InformationPlayer : MonoBehaviour {

	Transform playerTransform;
	Text PlayerInformation;

	// Use this for initialization
	void Start () {
		playerTransform = GameObject.Find ("Joueur").transform;
		PlayerInformation = GetComponent<Text> ();
	}
	
	// Update is called once per frame
	void Update () {
		string info = string.Format ("Position actuelle : {0} {1} {2} \n Rotation actuelle : {3} {4} {5}", 
						              playerTransform.position.x.ToString ("F1"),
						              playerTransform.position.y.ToString ("F1"),
						              playerTransform.position.z.ToString ("F1"),
						              playerTransform.rotation.x.ToString ("F1"),
						              playerTransform.rotation.y.ToString ("F1"),
						              playerTransform.rotation.z.ToString ("F1"));
		
		PlayerInformation.text = info;								
	}
}