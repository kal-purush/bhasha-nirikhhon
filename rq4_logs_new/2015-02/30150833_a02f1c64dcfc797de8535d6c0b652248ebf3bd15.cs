using UnityEngine;
using System.Collections;

public class GovnoScript_GravityDir : MonoBehaviour
{
	public GravityControl gc;

	private RectTransform rectTransform;
	private Vector3 initialScale;
	private Quaternion initialRotation;
	// Use this for initialization
	void Start ()
	{
		rectTransform = GetComponent<RectTransform>();
		initialRotation = rectTransform.rotation;
		initialScale = rectTransform.localScale;
	}
	
	// Update is called once per frame
	void Update ()
	{
		//rectTransform.localScale = new Vector3 (initialScale.x * gc., initialScale.y, initialScale.z);
		rectTransform.rotation = initialRotation * Quaternion.AngleAxis (gc.playerDeclinationAngle, new Vector3(0.0f, 0.0f, 1.0f));
	}
}