using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;

public class LevelLoader : MonoBehaviour {

	// Currently still testing level loading.
	void Start () {
        print("loading level");
        SceneManager.LoadScene(1);
	
	}
	
}