using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class SinnerCanvasController : MonoBehaviour {
		

	public Text text;
	public Sinner sinner;
	
	void Start(){
		sinner = transform.GetComponentInParent<Sinner> ();
	}

	// Update is called once per frame
	void Update () {
		//TODO update only when visible (text.isActive() not workin, dont know why, only returning false)
		string t = "";
		foreach (Sin sin in sinner.Sins) {
			t += "" + sin.SinType.ToString () + "  " + sin.Amount.ToString () + "\b";
		}
		text.text = t;


	}
}