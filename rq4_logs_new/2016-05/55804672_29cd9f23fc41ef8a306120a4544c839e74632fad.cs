using UnityEngine;
using System.Collections;

public class FollowObject : MonoBehaviour {

    public Transform ObjectToFollow;
    public float DistanceToObject;
    private Vector3 NewPosition = new Vector3();
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        NewPosition = this.transform.position;
        NewPosition.x = ObjectToFollow.position.x;
        NewPosition.z = DistanceToObject + ObjectToFollow.position.z;
        this.transform.position = NewPosition;
	}
}