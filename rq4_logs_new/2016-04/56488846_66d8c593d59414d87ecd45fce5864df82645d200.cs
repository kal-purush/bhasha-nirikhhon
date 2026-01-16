using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using UnityEngine.EventSystems;

public class AnalogueButtons : Button, ISelectHandler, IDeselectHandler {

	bool isSelected = false;
	public GameObject scriptObject;
	public string functionToCall = "OnClick";
	public string parameters = "";

	private float clickTimeout = 0.5f;
	private bool clicked = false;

	// Update is called once per frame
	void Update () {
		if (isSelected) {
			if(Input.GetAxis("TriggerSelect") >= 1)
				OnClick ();
		}
	}

	public void OnSelect (BaseEventData eventData) 
	{
		isSelected = true;
	}

	public void OnDeselect (BaseEventData data) 
	{
		isSelected = false;
	}

	public void OnClick()
	{
		if (!clicked) {
			scriptObject.SendMessage (functionToCall, parameters, SendMessageOptions.DontRequireReceiver);
			StartCoroutine(clickTimeOut());
		}
	}

	IEnumerator clickTimeOut()
	{
		clicked = true;
		yield return new WaitForSeconds (clickTimeout);
		clicked = false;
	}

}