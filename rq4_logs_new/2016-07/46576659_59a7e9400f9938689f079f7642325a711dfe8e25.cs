using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class CanvasHandler : MonoBehaviour {

	private CanvasScaler scaler;

	// Use this for initialization
	void Start () {
		scaler = GetComponent<CanvasScaler>();
	}

	public void ScaleCanvas() {
		scaler.uiScaleMode = CanvasScaler.ScaleMode.ScaleWithScreenSize;
	}
}