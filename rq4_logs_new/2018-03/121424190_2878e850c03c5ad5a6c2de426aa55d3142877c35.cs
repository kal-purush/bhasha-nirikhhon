using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class death : MonoBehaviour {

	//change scene
	public void changeScene(string loadScene){

		//reset all saved data
		PlayerPrefs.SetInt ("berries",5);
		PlayerPrefs.SetFloat ("xp", 0);
		PlayerPrefs.SetFloat ("health", 1);
		PlayerPrefs.SetInt ("level", 0);
		PlayerPrefs.SetInt ("steps", 0);

		System.DateTime epochStart = new System.DateTime(2018, 1, 1, 0, 0, 0, System.DateTimeKind.Utc);
		int cur_time = (int)(System.DateTime.UtcNow - epochStart).TotalHours;
		PlayerPrefs.SetFloat ("timeOfLastHealthCheck", cur_time);

		SceneManager.LoadScene(loadScene);
	}

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}