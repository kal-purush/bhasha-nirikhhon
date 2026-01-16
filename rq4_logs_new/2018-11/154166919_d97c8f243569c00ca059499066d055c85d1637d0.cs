using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DropItems : MonoBehaviour {
	const int ItemCount = 3;
	public GameObject[] items = new GameObject[ItemCount];
	private int DropCount = 0;//a item appear everytime player clicks
	private bool NeedFreeze = false;//if NeedFreeze == true, none of items can be triggered
	// Use this for initialization
	void Start () {
		//Debug.Log("BookShelf Initialized.");
	}
	
	private void OnMouseDown(){
		if(DropCount<ItemCount)
		items[DropCount++].SetActive(true);
	}

	// Update is called once per frame
	void Update () {
		foreach(GameObject item in items){
			if(!item.GetComponent<BoxCollider2D>().enabled){
				NeedFreeze = true;
			}
		}
		if(NeedFreeze){
			this.gameObject.GetComponent<BoxCollider2D>().enabled = false;
			foreach(GameObject item in items){
				//turn off box collider
				item.GetComponent<BoxCollider2D>().enabled = false;
			}
		}
	}
}