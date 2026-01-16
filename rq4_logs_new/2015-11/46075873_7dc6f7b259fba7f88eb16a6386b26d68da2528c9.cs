using UnityEngine;
using System.Collections;

public class animatoe : MonoBehaviour {
	Animator myanime; //assign via getcomponent 
	// Use this for initialization
	void Start () {
		myanime = GetComponent<Animator>();
	}
	
	// Update is called once per frame
	void Update () {
	if (Input.GetKey (KeyCode.Space)){
			myanime.SetBool ("ismobing", true);
			transform.Translate(new Vector3(-0.1f,0f,0f));}
			else {
			myanime.SetBool ("ismobing", false);
			}}
	}
