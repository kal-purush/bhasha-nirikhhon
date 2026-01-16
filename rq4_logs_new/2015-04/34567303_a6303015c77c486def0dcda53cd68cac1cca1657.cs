using UnityEngine;
using System.Collections;

public class coliderTriger : Photon.MonoBehaviour {

	public Transform explosionEffect;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		Debug.Log ("coliderScript");
		if (gameObject.GetComponent<FighterSettings> ().getHealth ()<1) {
			gameObject.GetComponent<PhotonView>().RPC("destroyFighter",PhotonTargets.All,null);	
		}
	
	}

	void OnTriggerEnter(Collider other) {
		Debug.Log ("hit");
		if (other.collider.name == "Fighter(Clone)") {
			gameObject.GetComponent<FighterSettings> ().addDamage (-100);
			Debug.Log ("fighter hit");
		}
		if (other.collider.name == "LaserShot(Clone)") {
				gameObject.GetComponent<FighterSettings> ().addDamage (-5);
				Debug.Log ("gun hit");
		}
	}
	[RPC]
	void destroyFighter(){
		Debug.Log ("destroy");
			Instantiate(explosionEffect,transform.position,transform.rotation);
			Destroy(gameObject);		

	}
}