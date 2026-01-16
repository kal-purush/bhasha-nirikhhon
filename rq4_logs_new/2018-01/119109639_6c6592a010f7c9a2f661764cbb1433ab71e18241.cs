using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class H1col : MonoBehaviour {
	public bool hCol;
	private int counter;
	// Use this for initialization
	void Start () {

	}

	// Update is called once per frame
	void Update () {

	}
	void OnTriggerStay2D(Collider2D checker)//, Collider2D wallcheckH)
	{
		//m_enemy = GameObject.Find("Bird");
		//print(name);
		if ((checker.gameObject.tag == "bg")){
			//print ("oh");
			counter += 1;
			if (counter >= 30) {
				counter = 0;
				hCol = false;
				//hCol = false;
			}
			//return;
		}
		if ((checker.gameObject.tag == "wall")) {
			//print ("oh");
			hCol = true;
			//hCol = true;
			//return;
		}
		/*
			if ((wallcheckH.gameObject.tag == "bg") || (wallcheckH2.gameObject.tag == "bg")){
				print ("counter");
				counter2 += 1;
				if (counter2 >= 30) {
					counter2 = 0;
					hCol = false;
				}
				//return;
			}
			if ((wallcheckH.gameObject.tag == "wall") || (wallcheckH2.gameObject.tag == "wall")) {
				print ("ah");
				hCol = true;
				//return;
			}
*/
	}
}