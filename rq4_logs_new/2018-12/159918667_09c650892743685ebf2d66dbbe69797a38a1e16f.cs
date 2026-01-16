using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using System;

/* ドロップを生成する */
public class CreateDrops : MonoBehaviour {

	public GameObject dropPrefab; //ドロッププレハブ
	public Sprite[] spriteDrop = new Sprite[5];

	private int dropMax =  44; //ドロップの最大個数
	private GameObject canvasGame; //Canvasゲーム
	private float screenW; //画面の幅
	private float screenH; //画面の高さ

	// Use this for initialization
	void Start () {
		canvasGame = GameObject.Find("CanvasGame");
		screenW = canvasGame.GetComponent<RectTransform>().sizeDelta.x;
		screenH = canvasGame.GetComponent<RectTransform>().sizeDelta.y;
		StartCoroutine(CreateDrop(dropMax));
	}

	public IEnumerator CreateDrop(int num){
		for(int i=0;i<num;i++){
			/* ドロップの生成 */
			GameObject drop = (GameObject)Instantiate(dropPrefab);
			//座標
			drop.transform.SetParent(this.transform,false);
			int deltaX = UnityEngine.Random.Range(-100,101);
			drop.transform.localPosition = new Vector3(deltaX,screenH/2, 0);	
			//Sprite
			int sC = spriteDrop.Length; //spriteCount
			int id = UnityEngine.Random.Range(0,sC);
			drop.GetComponent<Image>().sprite = spriteDrop[id];
			drop.name = "drop" + id.ToString();

			yield return new WaitForSeconds(0.05f);
		}
	}

	private IEnumerator DelayMethod(float waitTime, Action action)
	{
		yield return new WaitForSeconds(waitTime);
		action();
	}
}