using System.Collections;
using System.Collections.Generic;
using UnityEngine;

//用来实现当一个状态被激活时，
//这个物体也被切换到另一个状态。
//比如妈妈动怒时爸爸就该跪下落泪了

public class ChangeStateTogether : MonoBehaviour {
	public GameObject WithObject;
	public GameObject ToState;

	// Use this for initialization
	void Start () {
		ToState.SetActive(false);
	}

	private void ChangeState(){
		ToState.SetActive(true);
		this.gameObject.SetActive(false);
	}
	
	// Update is called once per frame
	void Update () {
		if(WithObject.activeSelf) ChangeState();
	}
}