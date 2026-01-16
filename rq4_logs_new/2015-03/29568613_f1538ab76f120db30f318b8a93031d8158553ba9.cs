using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class LoadingScreen : MonoBehaviour {

	public string SceneToLoad;
	 
	public GameObject background, bar;

	public MovieTexture movTexture;

	private float progress = 0;

	// Use this for initialization
	void Start () {
		movTexture.loop = true;
		background.SetActive (false);
		bar.SetActive (false);
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetKeyDown ("space"))
			StartCoroutine (DisplayLoadingScreen ());
	}

	IEnumerator DisplayLoadingScreen() {
		background.SetActive (true);
		bar.SetActive (true);

		movTexture.Play ();
		bar.transform.localScale = new Vector3( progress, bar.transform.localScale.y, bar.transform.localScale.z);

		AsyncOperation asy = Application.LoadLevelAsync (SceneToLoad);
		while (!asy.isDone) {
			progress = asy.progress * 100;
			movTexture.Play ();
			bar.transform.localScale = new Vector3( progress, bar.transform.localScale.y, bar.transform.localScale.z);
			yield return null;
		}
		movTexture.Stop ();
		background.SetActive (false);
		bar.SetActive (false);
	}
}