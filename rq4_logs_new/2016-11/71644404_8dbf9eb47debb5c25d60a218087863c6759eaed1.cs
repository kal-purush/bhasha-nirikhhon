using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;

public class DemoSwitch : MonoBehaviour {

	public bool Demo;
	public string nextlevel;

	// Update is called once per frame
	void Update () {
		if (Demo && Input.GetKeyDown ("p")) {
			SceneManager.LoadScene(nextlevel);
		}
	}
}