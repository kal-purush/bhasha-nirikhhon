using UnityEngine;
using System.Collections;

public class DaynNight : MonoBehaviour {
        private Camera cm;
    
	// Use this for initialization
	void Start () {
        cm = GetComponent<Camera>();
	}
	
	// Update is called once per frame
	void Update () {
        Color colour1 = Color.blue * Color.clear - Color.black + new Color(.3f, .4f, .6f);
        Color colour2 = Color.cyan * Color.white + new Color(.3f, .4f, .6f);
        float duration = 30.0f;

        cm.clearFlags = CameraClearFlags.SolidColor;
        float time = Mathf.PingPong(Time.time, duration) / duration;
        cm.backgroundColor = Color.Lerp(colour1, colour2, time);

	}
}