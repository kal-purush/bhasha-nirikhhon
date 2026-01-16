using UnityEngine;
using System.Collections;

public class PipeScript : MonoBehaviour {

	private SpriteRenderer pipeRenderer;
	void Start(){
		pipeRenderer = transform.Find ("PipeImage").GetComponent<SpriteRenderer> ();
	}

	public void SetHighLight(bool highlight){
		Color c = pipeRenderer.color;
		if (highlight) {
			c.a = 255;
		} else {
			c.a = 45;
		}
		pipeRenderer.color = c;
	}

	//TODO link on a better way, for no total overlapsing
	public void SetPositionAndScale(Transform roomFrom, Transform roomTo){
		transform.position = (roomTo.position - roomFrom.position) / 2F;
		transform.rotation = Quaternion.FromToRotation(roomFrom.position, roomTo.position);
		transform.localScale.Set(Vector3.Distance(roomFrom.position, roomTo.position), 1, 1);
	}
}