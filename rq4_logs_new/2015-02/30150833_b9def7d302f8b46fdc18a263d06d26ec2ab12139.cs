using UnityEngine;
using System.Collections;

public class AnimatedArrowScale : MonoBehaviour
{
	private RectTransform rectTransform;
	private Vector3 startScale;
	// Use this for initialization
	void Start ()
	{
		rectTransform = GetComponent<RectTransform>();
		startScale = rectTransform.localScale;
	}
	
	// Update is called once per frame
	void Update ()
	{
		rectTransform.localScale = new Vector3 (startScale.x * (Mathf.Sin(Time.timeSinceLevelLoad*5)*0.5f + 0.7f), startScale.y, startScale.z);
	}
}