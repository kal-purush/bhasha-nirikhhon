using UnityEngine;
using System.Collections;

/// <summary>
/// カメラコントローラークラス
/// 主にプレイシーンのプレイヤーを追尾する
/// </summary>
public class CameraController : MonoBehaviour {

	Vector3 offset = new Vector3 (0f, 1f, -5f);

	GameObject player;
	LimitLine limitLine;
	float sideBase = 10f;

	Vector3 point;
	LimitLine testLimit;

	// Use this for initialization
	void Awake () {
		player = GameObject.Find ("Player");
		limitLine = GameManager.GetInstance ().Limit;
		transform.position = new Vector3 (player.transform.position.x, player.transform.position.y + offset.y, offset.z);

		/*
		Camera c = GetComponent<Camera> ();
		Debug.Log ("c : " + c.transform.position);
		// 端っこをとる(現在は左だけ)
		var tmp = new Vector3 (0f, Screen.height / 2, (-c.transform.position.z));
		Debug.Log ("tmp : " + tmp);
		// スクリーン座標からワールド座標へ変換
		Vector3 p = c.ScreenToWorldPoint (tmp);
		Debug.Log ("p : " + p);
		// カメラから画面端の方向ベクトルを取得
		var dir = p - c.transform.position;
		Debug.Log ("dir : " + dir);
		sideBase = dir.magnitude * 2f;
		Debug.Log (sideBase);
		*/
		/*
		dispRay = new GameObject ();
		dispRay.AddComponent<DisplayRay> ();
		*/

	}

	// Update is called once per frame
	void Update () {


		/*
		Vector3 hitPoint = dispRay.GetComponent<DisplayRay> ().RangeCheck ();

		var pos = new Vector3 ();
		pos = new Vector3 (player.transform.position.x, player.transform.position.y + offset.y, offset.z);

		if (hitPoint == new Vector3(-1000f,-1000f,-1000f)) {
			pos.x = ((transform.position.x + 0.0000001f) - point.x);
		} else {
			point = hitPoint;
		}
		transform.position = pos;
		*/


		var pos = new Vector3 ();
		pos = new Vector3 (player.transform.position.x, player.transform.position.y + offset.y, offset.z);

		if (player.transform.position.x < (limitLine.Left + sideBase)) {
			pos.x = limitLine.Left + sideBase;
		}
		if (player.transform.position.x > (limitLine.Right - sideBase)) {
			pos.x = limitLine.Right - sideBase;
		} 

		transform.position = pos;

	}
}