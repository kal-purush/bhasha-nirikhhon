using UnityEngine;
using System.Collections;

public class UIShowHide : MonoBehaviour {

	CanvasRenderer myRenderer;

	void Start() {
		myRenderer = GetComponent<CanvasRenderer> () as CanvasRenderer;
		myRenderer.SetAlpha (0f);
	}

	public void show() {
		CanvasRenderer myRenderer = GetComponent<CanvasRenderer> () as CanvasRenderer;
		myRenderer.SetAlpha (1f);
	}

	public void hide() {
		CanvasRenderer myRenderer = GetComponent<CanvasRenderer> () as CanvasRenderer;
		myRenderer.SetAlpha (0f);
	}
}