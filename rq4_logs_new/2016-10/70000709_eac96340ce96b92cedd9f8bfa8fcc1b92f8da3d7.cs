using UnityEngine;
using System.Collections;

public class TargetShooter : MonoBehaviour {

	public float speed = 30f;
	public Transform barrel;
	public Transform target;

	// Use this for initialization
	void Start () {
		barrel = this.transform.GetChild(0);
		target = GameObject.Find("Player").transform;
	}
	
	// Update is called once per frame
	void Update () {
		Quaternion rotation = Quaternion.LookRotation(target.position - barrel.position);
		rotation.x = 0;
		rotation.y = 0;
		barrel.rotation = Quaternion.RotateTowards(barrel.rotation, rotation, speed * Time.deltaTime);
	}
}