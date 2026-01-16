using UnityEngine;
using System.Collections;

public class PrekazkaScript : MonoBehaviour {

    private bool isRekt = false;
    private Kolajnice kolajnice;

	// Use this for initialization
    void Start()
    {
        kolajnice = GameObject.FindGameObjectWithTag("KolajniceTag").GetComponent<Kolajnice>();
	}
	
	// Update is called once per frame
	void Update () {
        if (isRekt)
        {
            this.transform.position += Vector3.right * 2;
        }
        if (this.transform.position.z > 2000) Destroy(this);
	}

    public void GetRekt()
    {
        isRekt = true;
    }

    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "Player")
        {
            //kolajnice.GameOver = true;
        }
    }
}