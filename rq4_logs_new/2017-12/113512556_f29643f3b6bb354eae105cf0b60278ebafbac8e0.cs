using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CubeBehavior : MonoBehaviour {
	public bool isColored;
	public bool isSelected;
	public bool isBlacked;
	public int cubeX, cubeY;
	GameController myGameController;

	// Use this for initialization

	void Start () {
		isSelected = false;

		myGameController = GameObject.Find ("GameObject").GetComponent<GameController> ();
	}

	public void OnMouseDown () {
		myGameController.ProcessClick (gameObject, cubeX, cubeY, gameObject.GetComponent<Renderer> ().material.color); 
	}
	// Update is called once per frame
	void Update () {
		
	}
}