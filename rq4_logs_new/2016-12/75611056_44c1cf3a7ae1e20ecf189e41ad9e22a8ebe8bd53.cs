using UnityEngine;
using System.Collections;

public class FallingTree : MonoBehaviour {

    [SerializeField] float fallAngle = 90f;
    [SerializeField] float smooth = 2f;
    bool enterCollider = false;


	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	    if(enterCollider == true)
        {
            var target = Quaternion.Euler(fallAngle, 0, 0);
            transform.localRotation = Quaternion.Slerp(transform.localRotation, target, Time.deltaTime * smooth);

        }
    }

    void OnTriggerEnter(Collider col)
    {
        if(col.gameObject.tag == "Player")
        {
            enterCollider = true;
            Debug.Log("Hit It");
        }
    }

}