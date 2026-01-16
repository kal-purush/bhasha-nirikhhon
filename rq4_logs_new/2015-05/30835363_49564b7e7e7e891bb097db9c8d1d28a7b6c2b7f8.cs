using UnityEngine;
using System.Collections;

[ExecuteInEditMode]
public class LayerController : MonoBehaviour {

	public bool activate = false;
	
	// Update is called once per frame
	void Update () {
		if (!Application.isPlaying && activate) 
		{
			GameObject[] props = GameObject.FindGameObjectsWithTag("Layer");

			for (int i = 0; i < props.Length; i++)
				props[i].transform.localPosition = new Vector3(props[i].transform.localPosition.x, 
				                                               props[i].transform.localPosition.y, 
				                                               props[i].transform.localPosition.y / 100.0f - 1.0f);
			activate = false;

		}
	}
}