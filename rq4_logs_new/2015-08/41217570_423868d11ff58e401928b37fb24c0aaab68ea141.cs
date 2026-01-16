using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class BuyRow : MonoBehaviour {


	public Transform nameObject;
	public Transform countObject;
	public Transform valueObject;


	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}


	public void setData(string name, int count, int value){
		this.setName (name);
		this.setCount (count);
		this.setValue (value);
	}

	public void setName(string name){
		nameObject.GetComponent<Text> ().text = name;
	}
	public void setCount(int count){
		countObject.GetComponent<Text> ().text = "x " + count.ToString ();
	}
	public void setValue(int value){
		valueObject.GetComponent<Text> ().text = value.ToString ("C");
	}
}