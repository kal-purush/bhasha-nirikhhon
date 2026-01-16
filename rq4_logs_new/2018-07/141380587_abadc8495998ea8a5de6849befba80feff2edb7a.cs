using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ItemController : MonoBehaviour {

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    void OnTriggerEnter(Collider other)
    {
        if(other.name == "Player")
        {
            Destroy(gameObject);
            GameObject effectObj = gameObject.transform.parent.Find("ItemEffect(Clone)").gameObject;
            Destroy(effectObj);
        }
    }
}