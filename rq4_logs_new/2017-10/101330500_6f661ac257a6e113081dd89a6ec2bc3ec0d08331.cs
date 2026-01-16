using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ReachCollider : MonoBehaviour {

    public Material outOfBounds;

    public Material inBounds;

    public GameObject cameraRig;

    private BoxCollider bc;

	// Use this for initialization
	void Start () {
        bc = gameObject.GetComponent<BoxCollider>();

        bc.center = new Vector3(cameraRig.transform.position.x, 
            cameraRig.transform.position.y + GameControl.Instance.heightMax / 2, 
            cameraRig.transform.position.z);
        bc.size = new Vector3(System.Math.Abs(GameControl.Instance.leftMax) 
            + GameControl.Instance.rightMax, GameControl.Instance.heightMax *2f,
            System.Math.Abs(GameControl.Instance.leftMax) + GameControl.Instance.rightMax);

	}
	
	// Update is called once per frame
	void Update () {
		
	}

    public void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.CompareTag("Ball"))
        {
            ToInBounds(other.gameObject);
        }
    }

    public void OnTriggerStay(Collider other)
    {
        if (other.gameObject.CompareTag("Ball"))
        {
            ToInBounds(other.gameObject);
        }
    }

    public void OnTriggerExit(Collider other)
    {
        if (other.gameObject.CompareTag("Ball"))
        {
            ToOutofBounds(other.gameObject);
        }
    }

    private void ToInBounds(GameObject ball)
    {
        ball.GetComponent<Renderer>().material = inBounds;
    }

    private void ToOutofBounds(GameObject ball)
    {
        ball.GetComponent<Renderer>().material = outOfBounds;
    }
}