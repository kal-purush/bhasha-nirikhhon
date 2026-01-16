using UnityEngine;
using System.Collections;

public class AnimatedArrowScale : MonoBehaviour
{
	private RectTransform rectTransform;
	private Vector3 startPosition;
	// Use this for initialization
	void Start ()
	{
		rectTransform = GetComponent<RectTransform>();
        startPosition = rectTransform.position;
	}
	
	// Update is called once per frame
	void Update ()
	{
	    float time = Time.timeSinceLevelLoad*5;
        Vector3 newPos = new Vector3(
            startPosition.x + (Mathf.Sin(Time.timeSinceLevelLoad*5f)*rectTransform.rect.width/2),
            startPosition.y, startPosition.z);

	    rectTransform.position = newPos;
	}
}