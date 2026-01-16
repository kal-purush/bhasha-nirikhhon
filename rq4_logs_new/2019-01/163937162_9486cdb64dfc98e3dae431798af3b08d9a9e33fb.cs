using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class goscript : MonoBehaviour {

	public GameObject target;

	bool test = false;
	Vector3 mousePos;
	Vector3 offSet;
	Vector3 clickPos;
	
	bool dragMode = false;
	
	// Use this for initialization
	void Start () {
		Debug.Log("処理開始");
	}
	
	// Update is called once per frame
	void Update () {
		if (test == false) {
			transform.position += transform.forward *= 2.0f;
		}
		if (Input.GetMouseButtonDown(0)) {
			Debug.Log("クリックした瞬間");
			// test = !test;
			GameObject magicCircle = Instantiate (target, new Vector3 (transform.position.x, transform.position.y, transform.position.z), Quaternion.identity);
			magicCircle.transform.position += transform.forward *= 2.0f;
			// magicCircle.transform.Rotate(Vector3.up * 90);
			magicCircle.transform.Rotate(Vector3.right * 90);
			// magicCircle.transform.Rotate(Vector3.up * 90);
			
			clickPos = Input.mousePosition; //ドラッグ開始位置を取得して、
			offSet = transform.position - clickPos; //UIの中心とドラッグ開始位置のズレを取得
			
			dragMode = true; //ドラッグ処理を開始
		}
		
		if (Input.GetMouseButtonUp(0)) {
		    Debug.Log("離した瞬間");
		    dragMode = false; //ドラッグ処理を終了
		}

		if (Input.GetMouseButton(0)) {
		    Debug.Log("クリックしっぱなし");
		}
		
		if (dragMode){
			PanelDrag();
		}
	}
	void PanelDrag() {
		 // GameObject magicCircle2 = Instantiate (target, new Vector3 (transform.position.x, transform.position.y, transform.position.z), Quaternion.identity);
		// magicCircle2.position += new Vector3( e.delta.x, e.delta.y, 0f );
		mousePos = Input.mousePosition; //ドラッグ中のカーソル位置

        transform.position = mousePos + offSet; //UIをドラッグさせる
	}
}