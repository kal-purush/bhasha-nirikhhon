using UnityEngine;
using System.Collections;

public class LaserTrigger : MonoBehaviour {
	/*
		Butuh Collider (Box/Sphere/Capsule) di 'laser' dan 'player'
	*/
	//Trigger for On/Off the Laser
	public bool trigger;
	public LineRenderer laser2;

	void Start(){
		//Inisiasi awal, laser pasti nyala
		trigger = true;
	}

	//Function for Collision Player and Laser
	void OnTriggerEnter(Collider other){
		if(trigger==true){
			if(other.gameObject.tag == "Player")
			{
				gameObject.SendMessage("Death"); //call fucntion Death on Player Script
				SwitchOff();
			}
		}else if(trigger==false){
		}
	}
	//Switch On Laser
	void SwitchOn(){
		trigger=true;
		laser2.enabled = true; //line renderer enable
	}
	//Switch Off Laser
	void SwitchOff(){
		trigger=false;
		laser2.enabled = false; //line renderer unable
	}
}