using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour {

	public ItemBox[] ItemBoxs;
	// Use this for initialization

	public bool isGameOver;
	void Start () {
		isGameOver = false;
	}
	
	// Update is called once per frame
	void Update () {
		if(isGameOver = true){
			return;
		}
		
		int count=0;
		for(int i = 0;i<ItemBoxs.Length;i++){
			if(ItemBoxs[i].isOveraped == true){
				count++;
			}
		}
		if(count == 3){
			isGameOver = true;
		}
	}
}