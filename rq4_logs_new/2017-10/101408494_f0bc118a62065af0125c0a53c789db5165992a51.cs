using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CheckPlayerCount : MonoBehaviour {

	public void redirectToGame(){
		GameObject playerList = GameObject.Find("PlayersList");
		int count = playerList.transform.childCount;
		if(count < 7 && count > 2){
			this.GetComponent<ChangeSceneButton>().changeSceneOnClick();
		}
	}
}