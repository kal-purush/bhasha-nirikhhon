using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyController : MonoBehaviour {

    Animation anim;
    BoxCollider boxCollider;
	// Use this for initialization
	void Start () {
        anim = GetComponent<Animation>();
        boxCollider = GetComponent<BoxCollider>();
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    void OnTriggerEnter(Collider other)
    {
        print(other.name);
        if(other.name == "cutter01")
        {
            anim.Play("Allosaurus_Die");
            Destroy(boxCollider);
        } 
    }
}