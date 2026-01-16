using UnityEngine;
using System.Collections;

public class Placement : MonoBehaviour {

    // si le joueur est en train de placer le block
    protected bool moving;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        Vector3 pos = Camera.main.ScreenToWorldPoint(Input.mousePosition); ;
        pos.z = 0;
        if (moving)
            this.transform.position = pos;
	}

    public void SetMoving(bool value)
    {
        moving = value;
    }
}