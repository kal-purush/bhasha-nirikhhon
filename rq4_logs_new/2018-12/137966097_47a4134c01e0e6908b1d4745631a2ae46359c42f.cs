using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SetMarbleNumber : MonoBehaviour {

	// Use this for initialization
	void Start () {
        //Debug.Log("SetMarbleNumber");
        //Debug.Log(GlobalParameters.marble_num);
        gameObject.GetComponent<Text>().text = GlobalParameters.marble_num.ToString();
		
	}

    private void Awake()
    {
        gameObject.GetComponent<Text>().text = GlobalParameters.marble_num.ToString();
    }

    // Update is called once per frame
    void Update () {
        gameObject.GetComponent<Text>().text = GlobalParameters.marble_num.ToString();
    }
}