using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class SliderController : MonoBehaviour {
	public Slider slider;
	public Image handle;
	public Image emptyHandle;

	float handleSize = 12f;
	bool hold;

	void Start () {
		//Set color at start
		Color c = handle.color;
		c.a = slider.value <= 0 ? 0f : 1f;
		handle.color = c;
	}
	
	void Update () {
		//Update color
		Color c = handle.color;
		c.a = Mathf.Lerp(handle.color.a, slider.value <= 0 ? 0f : 1f, Time.deltaTime * 16f);
		handle.color = c;

		//Update size
		handleSize = Mathf.Lerp(handleSize, hold ? 18f : 12f, Time.deltaTime * 20f);
		emptyHandle.rectTransform.sizeDelta = new Vector2(handleSize, handleSize);
	}

	public void PointerDown() {
		hold = true;
	}

	public void PointerUp() {
		hold = false;
	}
}