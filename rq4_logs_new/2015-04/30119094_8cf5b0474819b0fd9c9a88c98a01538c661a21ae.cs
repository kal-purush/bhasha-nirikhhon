using UnityEngine;
using System.Collections;
 
 /* code from: http://www.devination.com/2014/01/unity-gui-tutorial-scale-gui-to-right.html */
public class GUIAspectRatio : MonoBehaviour {

	public Vector2 scaleOnRatio1 = new Vector2(0.1f, 0.1f);
	private Transform myTrans;
	private float widthHeightRatio;

	// Use this for initialization
	void Start () {
		myTrans = transform;
		SetScale();
	}
	
	void SetScale() {
 		widthHeightRatio = (float)Screen.width/Screen.height;
 		//Apply the scale. We only calculate y since our aspect ratio is x (width) authoritative: width/height (x/y)
  		myTrans.localScale = new Vector3 (scaleOnRatio1.x, widthHeightRatio * scaleOnRatio1.y, 1);
 	}

}