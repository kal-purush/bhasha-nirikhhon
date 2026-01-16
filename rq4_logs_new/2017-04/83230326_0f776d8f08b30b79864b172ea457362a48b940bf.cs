using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;
public class TestMoves : MonoBehaviour {
    public string[] testKeys;
    public UnityEvent successEvent;
	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        bool success = true;
		foreach (string key in testKeys) {
            if (!Input.GetButtonDown(key)) {
                success = false;
            }
        }

        if (success == true) {
            print("SUCCESS");
            successEvent.Invoke();

        }
	}
}