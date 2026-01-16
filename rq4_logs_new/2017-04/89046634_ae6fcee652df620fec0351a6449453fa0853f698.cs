using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PlayerController : MonoBehaviour {

	public GameObject player1;
	public GameObject player2;

	public Button upP1;
	public Button upP2;
	public Button downP1;
	public Button downP2;
	public Button shootP1;
	public Button shootP2;

	// Use this for initialization
	void Start () {
	}

	// Update is called once per frame
	void Update () {
	}

	public void p1Up(){
		if(player1.gameObject.transform.position.y<=3.55f){
		player1.gameObject.transform.position = new Vector3 (
		player1.gameObject.transform.position.x
		,player1.gameObject.transform.position.y+1
		,player1.gameObject.transform.position.z
		);}
	}
	public void p1Down(){
		if (player1.gameObject.transform.position.y>=-0.86f) {
			player1.gameObject.transform.position = new Vector3 (
			player1.gameObject.transform.position.x
			,player1.gameObject.transform.position.y-1
			,player1.gameObject.transform.position.z
			);}
	}
	public void p2Up(){
		if (player2.gameObject.transform.position.y<=3.55f) {
			player2.gameObject.transform.position = new Vector3 (
			player2.gameObject.transform.position.x
			,player2.gameObject.transform.position.y+1
			,player2.gameObject.transform.position.z
			);}
	}
	public void p2Down(){
		if (player2.gameObject.transform.position.y>=-0.86f) {
			player2.gameObject.transform.position = new Vector3 (
				player2.gameObject.transform.position.x
				,player2.gameObject.transform.position.y-1
				,player2.gameObject.transform.position.z
			);}
	}

	public void shotP1(){
		
	}
}