using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WaterDrop : MonoBehaviour {
    public GameObject wateringCan;
    bool ignoreCollision = false;
	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    public void callIgnoreCollision(GameObject theWateringCan)
    {
        Physics.IgnoreCollision(theWateringCan.GetComponent<Collider>(), GetComponent<Collider>());
        ignoreCollision = true;
    }
    public void OnCollisionEnter(Collision collision)
    {
        if (collision.gameObject.GetComponent<FarmBlockInfo>())
        {
            collision.gameObject.GetComponent<FarmBlockInfo>().waterCount += 1;
            Destroy(gameObject);
        }
        else
        {
            if (ignoreCollision)
            {
                Destroy(gameObject);
            }
        }
    }
}