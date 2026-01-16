using UnityEngine;
using System.Collections;

public class bottle_puzzle : MonoBehaviour {
	
	public GameObject empty_bottle;
	public GameObject right;
	public GameObject wrong;
	public GameObject bottle;
	int chk=0;
	

	/*
	void Update () {
		Vector3 lp = bottle.transform.position;
		float dis = Vector3.Distance (empty_bottle.transform.position, lp);

		if (dis < 3f) {
		//	GameObject.Find("EventSystem").GetComponent<on_correcteffect>().on_eff();
			if(Input.GetKeyUp(KeyCode.Mouse0)){
				if(bottle.GetComponent<bottle_kind>().kind==0);{
					wrong.SetActive(true);
				}

				if(bottle.GetComponent <bottle_kind>().kind==1);{
					if(chk==2){
						wrong.SetActive(false);
						right.SetActive(true);}
					else{
						wrong.SetActive(true);
						chk=1;}
				}
				if(bottle.GetComponent <bottle_kind>().kind==2);{
					if(chk==1){
						wrong.SetActive(false);
						right.SetActive(true);}
					else{
						wrong.SetActive(true);
						chk=2;}
				}

		//		GameObject.Find("EventSystem").GetComponent<on_correcteffect>().StopCoroutine ("on_effect");
			}
		}
		
	}*/
}