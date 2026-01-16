using UnityEngine;
using System.Collections;

public class Health : Photon.MonoBehaviour {
	
	public Transform explosionEffect;
	public int health;
	void Awake(){
		health = gameObject.GetComponent<FighterSettings> ().health; 
	
	}
	[RPC]
	public void addDamage(int num){
		health += num;
	}
	public int getHealth(){
		return health;
	}
	
	void destroyFighter(){
		Debug.Log ("destroy");
		Instantiate(explosionEffect,transform.position,transform.rotation);
		if(gameObject.GetComponent<PhotonView>().instantiationId==0){
			Destroy(gameObject);
		}else{
			if(PhotonNetwork.isMasterClient){
				PhotonNetwork.Destroy (gameObject);
			}
		}
		
	}
}