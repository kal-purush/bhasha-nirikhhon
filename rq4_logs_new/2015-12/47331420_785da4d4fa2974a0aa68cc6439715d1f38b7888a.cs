using UnityEngine;
using System.Collections;

public class acid_to_fountain : MonoBehaviour {
	
	public GameObject bottle;
	public GameObject fountain;
	public bool acid_chk=false;
	
	void Update () {
		Vector3 lp = bottle.transform.position;
		float dis = Vector3.Distance (fountain.transform.position, lp);

		if (dis < 3f) {
			GameObject.Find("EventSystem").GetComponent<on_correcteffect>().on_eff();
			if(Input.GetKeyUp(KeyCode.Mouse0)){				
				acid_chk=true;
				GameObject.Find ("item").GetComponent <manage_inven> ().pop_item (bottle);
				GameObject.Find ("blank").GetComponent<inven> ().dragitem = false;
				GameObject.Find("EventSystem").GetComponent<on_correcteffect>().StopCoroutine ("on_effect");
			}
		}
		
	}
	
}