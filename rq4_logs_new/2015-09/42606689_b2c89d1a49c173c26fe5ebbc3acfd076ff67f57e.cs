using UnityEngine;
using System.Collections;

public class BlackHole : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

    void OnCollisionEnter(Collision col)
    {
        if(col.gameObject.name == "Ball")
        {
            Destroy(col.gameObject);
            Application.LoadLevel("MariaTest");
        }
    }
}