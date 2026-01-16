using System.Collections;
using System.Collections.Generic;
using Fungus;
using UnityEngine;

public class flavorText : MonoBehaviour {

    //public string[] midStateFlavor = new string[4] { "mphrase 1", "mphrase 2", "mphrase 3" "mphrase 4" };
    //public string[] lowStateFlavor = new string[4] { "lphrase 1", "lphrase 2", "lphrase 3" "lphrase 4" };
    //public string[] hiStateFlavor = new string[4] { "hphrase 1", "hphrase 2", "hphrase 3" "hphrase 4" };


    // Use this for initialization
    void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}

   public void flavorPicker (){

        GetComponent<StateTracker>().myFlowchart.SetIntegerVariable("flavorNumber", (Random.Range(0, 2)));
    }
}