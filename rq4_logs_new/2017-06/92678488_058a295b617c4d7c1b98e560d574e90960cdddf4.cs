using UnityEngine;
using System.Collections;

public class SlashCore : MonoBehaviour
{
	GameObject obj;
	//public GameObject parent;

	Vector3 v;
	Transform target;

	void Start () 
	{
		Destroy(gameObject,9.5f);
		transform.Rotate(0,0,80f);

		v.Set(0,2f,0);

		obj = GameObject.Find("Player");
		target = obj.GetComponent<Transform>();

		transform.position = target.position + v;
	}

	void LateUpdate()
	{
		transform.RotateAround(target.position, new Vector3(0,0,1), 10f);
	}

	void OnTriggerEnter2D(Collider2D other) 
	{
		if (other.gameObject.tag == "Enemy")
		Destroy(gameObject,0.0f);
	}
}