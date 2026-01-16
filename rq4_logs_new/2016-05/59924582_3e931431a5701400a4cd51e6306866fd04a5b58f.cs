using UnityEngine;
using System.Collections;

public class CannonDestroy : MonoBehaviour {
    Rigidbody rb;
    Component[] gun;

    void OnCollisionEnter(Collision col)
    {
       // if (col.gameObject.name == "projectile") 
       // {
            gun = GetComponentsInChildren<Rigidbody>();
            foreach (Rigidbody thing in gun)
            {
                thing.isKinematic = false;
            }
            rb = GetComponent<Rigidbody>();
            rb.isKinematic = false;
            GetComponent<Transform>().DetachChildren();
            //Destroy(col.gameObject);
        //}
    }
    // Use this for initialization
    void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}