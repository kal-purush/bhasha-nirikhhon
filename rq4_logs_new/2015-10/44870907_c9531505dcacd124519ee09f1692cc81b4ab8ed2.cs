using UnityEngine;
using System.Collections;

public class AddForce : MonoBehaviour {

	// Use this for initialization
	public float forceAmt;
	public Vector3 startPos;
	public Quaternion startRot;
	public Rigidbody rbody;
	public MeshCollider collider;
	public float duration=5f;
	void Start () {
		collider=GetComponent<MeshCollider>();
		startRot=transform.rotation;
		rbody=GetComponent<Rigidbody>();
		startPos=transform.position;
	}

	// Update is called once per frame
	void Update () {
		if(Input.GetKeyDown(KeyCode.A)){
			rbody.AddForce(Vector3.up*forceAmt);
		}

		if(Input.GetKeyDown(KeyCode.U)){
			StartCoroutine("GoopBack");
		}
	}

	IEnumerator GoopBack(){
		float i = 0f;
		Vector3 tPos=transform.position;
		Quaternion tRot=transform.rotation;
		Destroy(rbody);
		Destroy(GetComponent<MeshCollider>());
		while(i<=1f){
			Quaternion rot = Quaternion.Lerp(tRot,startRot,i);
			Vector3 pos = Vector3.Lerp(tPos,startPos,i);
			transform.position=pos;
			transform.rotation=rot;
			i+=Time.deltaTime/duration;
			yield return null;
		}
		gameObject.AddComponent<Rigidbody>();
		GetComponent<Rigidbody>().useGravity=false;
		gameObject.AddComponent<MeshCollider>();
		GetComponent<MeshCollider>().convex=true;
		transform.rotation=startRot;
		transform.position=startPos;
	}
}