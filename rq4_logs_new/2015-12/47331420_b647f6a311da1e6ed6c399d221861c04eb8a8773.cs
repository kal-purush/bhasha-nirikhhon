using UnityEngine;
using System.Collections;

public class bat_to_man : MonoBehaviour {

	public GameObject bat;
	public GameObject lying_man;
	public GameObject finding_man;
	void Update(){
		
		Vector3 lp = finding_man.transform.position;
		float dis = Vector3.Distance (bat.transform.position, lp);
		
		
		if (dis < 2f) {
			if (GameObject.Find ("Boy").GetComponent<player_state> ().his_position == 5 && finding_man.activeSelf){
				GameObject.Find("EventSystem").GetComponent<on_correcteffect>().on_eff();
				if(Input.GetKeyUp(KeyCode.Mouse0)){

					StartCoroutine("hit_the_man");
					
				}
			}
		}
		
	}
	IEnumerator hit_the_man(){
		for (int i=0; i<50; i++) {
			finding_man.transform.Translate(new Vector3(-0.05f,0f,0f));
			finding_man.transform.Rotate(new Vector3(0f,0f,0.5f));
			yield return new WaitForSeconds(0.02f);
		}
		
		finding_man.SetActive (false);
		lying_man.SetActive (true);
		
		GameObject.Find ("blank").GetComponent<inven> ().dragitem = false ;
		GameObject.Find ("item").GetComponent <manage_inven> ().pop_item (bat.gameObject);
	}
}