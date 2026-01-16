using UnityEngine;
using System.Collections;

public class sculpture_touch : MonoBehaviour {

	public GameObject[] trueending = new GameObject[15];
	public GameObject normalending;
	public GameObject[] badending = new GameObject[4];
	void OnMouseUpAsButton(){
		if (GameObject.Find ("Boy").GetComponent<player_manager> ().use_devil >= 2) {
			//badending
			StartCoroutine("badend");
		} else if (GameObject.Find ("soulstone_manager").GetComponent<soulstone_manage> ().temple != 2
			&& GameObject.Find ("soulstone_manager").GetComponent<soulstone_manage> ().residence != 2
			&& GameObject.Find ("soulstone_manager").GetComponent<soulstone_manage> ().palace != 2
			&& GameObject.Find ("soulstone_manager").GetComponent<soulstone_manage> ().woodman != 2) {
			StartCoroutine("trueend");
			//trueending
		} else {
			StartCoroutine("normalend");
			//normalending
		}
	}

	IEnumerator badend(){
		yield return new WaitForSeconds (1f);
		badending [0].SetActive (true);
		for (int i=1; i<4; i++) {
			yield return new WaitForSeconds (3f);
			badending [i].SetActive (true);
			badending [i-1].SetActive (false);

		}
		yield return new WaitForSeconds (4f);
		Application.Quit();

	}
	IEnumerator trueend(){
		yield return new WaitForSeconds (1f);
		trueending [0].SetActive (true);
		for (int i=1; i<14; i++) {
			yield return new WaitForSeconds (3f);
			trueending [i].SetActive (true);
			trueending [i-1].SetActive (false);
			
		}
		yield return new WaitForSeconds (4f);
		Application.Quit();
		
	}
	IEnumerator normalend(){
		yield return new WaitForSeconds (1f);
		normalending.SetActive (true);
		yield return new WaitForSeconds (4f);
		Application.Quit();
		
	}
}