using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class MoniesController : Singleton<MoniesController> {
	public GameObject moniesTextGameObject;
	protected Text moniesText;

	public GameObject deltaMoniesTextGameObject;
	protected Text deltaMoniesText;

	public float cash = 1000f;
	public float deltaCash = -2f;  // Let's build more advanced delta management (different timers, ...) later

	protected void UpdateUI() {
		moniesText.text = "$" + cash.ToString ();

		if (deltaCash >= 0) {
			deltaMoniesText.text = "+";
			deltaMoniesText.color = Color.green;
		} else {
			deltaMoniesText.text = "";
			deltaMoniesText.color = Color.red;
		}
		deltaMoniesText.text += deltaCash.ToString ();
	}

	void Start() {
		moniesText = moniesTextGameObject.GetComponent<Text> ();
		deltaMoniesText = deltaMoniesTextGameObject.GetComponent<Text> ();
		StartCoroutine (Tick ());
	}

	IEnumerator Tick() {
		WaitForSeconds wait = new WaitForSeconds (1);
		// TODO: stop on pause, level done, etc.
		while (true) {
			cash += deltaCash;
			UpdateUI ();
			yield return wait;
		}
	}
}