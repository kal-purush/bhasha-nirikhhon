using UnityEngine;
using System.Collections;
using UnityEngine.Advertisements;

public class BaseGameController : MonoBehaviour {

	private float width = 640f;
	private float height = 960f;
	private float pixelPerUnit = 100f;

	protected void setupWindow(Camera cam) {
		float aspect = (float)Screen.height / (float)Screen.width;
		float bgAcpect = height / width;
		
		cam.orthographicSize = (height / 2f / pixelPerUnit);
		
		if (bgAcpect > aspect) {
			float bgScale = height / Screen.height;
			float camWidth = width / (Screen.width * bgScale);
			cam.rect = new Rect ((1f - camWidth) / 2f, 0f, camWidth, 1f);
		} else {
			float bgScale = width / Screen.width;
			float camHeight = height / (Screen.height * bgScale);
			cam.rect = new Rect (0f, (1f - camHeight) / 2f, 1f, camHeight);
		}

	}

	protected void initAds() {
		if (Advertisement.isSupported) {
			Advertisement.allowPrecache = true;
			Advertisement.Initialize ("64783"); //Movie
		} else {
			Debug.Log("Platform not supported");
		}
	}
}