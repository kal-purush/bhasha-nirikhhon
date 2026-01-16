using UnityEngine;
using System.Collections;

public class Building : MonoBehaviour {

	// Use this for initialization
    //void Start () {
	
    //}
	
    //// Update is called once per frame
    //void Update () {
	
    //}

    void onCollisionEnter(Collider obj)
    {
        if (obj.tag == "builder")
        {
            this.transform.rotation = obj.transform.rotation;
            this.gameObject.transform.Translate(obj.transform.position.x, obj.transform.position.y, this.gameObject.transform.position.z);
            Debug.Log("They should be stacked");
        }
    }
}