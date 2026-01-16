using UnityEngine;
using System.Collections;

[RequireComponent(typeof(LineRenderer))]

public class BalistaString : MonoBehaviour {

	public Transform armPoint1;
	public Transform armPoint2;
	public Transform pullBack;

	public LineRenderer renderer;


	// Use this for initialization
	void Start () {
		if (renderer == null) {
			renderer = GetComponent<LineRenderer>()as LineRenderer;
		}
		renderer.SetVertexCount (3);
	}
	
	// Update is called once per frame
	void Update () {
		renderer.SetPosition (0, armPoint1.position);
		renderer.SetPosition (1, pullBack.position);
		renderer.SetPosition (2, armPoint2.position);

	}
}