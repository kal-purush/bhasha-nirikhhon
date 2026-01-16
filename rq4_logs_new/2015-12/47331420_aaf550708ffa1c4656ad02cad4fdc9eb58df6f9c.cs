using UnityEngine;
using System.Collections;

public class item_info_residence : MonoBehaviour {
	public int item_number;
	
	void OnMouseEnter(){
		if(GameObject.Find ("blank").GetComponent<inven> ().dragitem ==false )
			GameObject.Find ("Text_item").GetComponent<itemtext_manager_residence> ().item_info_set (item_number, gameObject.transform.position);
		else 
			GameObject.Find ("Text_item").GetComponent<itemtext_manager_residence> ().item_info_off ();
	}
	void OnMouseExit(){
		GameObject.Find ("Text_item").GetComponent<itemtext_manager_residence> ().item_info_off ();
	}
}