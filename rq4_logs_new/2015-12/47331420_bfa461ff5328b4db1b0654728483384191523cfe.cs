using UnityEngine;
using System.Collections;

public class cut_cutton : MonoBehaviour {
	public GameObject cutton;
	public GameObject cutton_item;
	
	void Update () {
		Vector3 lp = cutton.transform.position;
		float dis = Vector3.Distance (transform.position, lp);
		
		if (dis < 1f ) {
			if (GameObject.Find ("Boy").GetComponent<player_state> ().his_position == 14){
				GameObject.Find("EventSystem").GetComponent<on_correcteffect>().on_eff();
				if(Input.GetKeyUp(KeyCode.Mouse0)){
					
					
					GameObject.Find ("item").GetComponent <manage_inven> ().pop_item(this.gameObject);
					GameObject.Find ("item").GetComponent <manage_inven> ().push_item(cutton_item);
					GameObject.Find ("blank").GetComponent<inven> ().dragitem = false ;
				}
			}
		}
		
	}
	
}