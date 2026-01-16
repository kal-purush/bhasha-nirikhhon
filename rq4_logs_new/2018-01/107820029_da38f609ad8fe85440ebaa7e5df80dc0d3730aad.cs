using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class DeathControllerScript : MonoBehaviour {

    private string level;
    public string startingLevel;

    public KeyCode reviveKey;
    public KeyCode resetKey;

	// Use this for initialization
	void Start () {
        DontDestroyOnLoad(this.gameObject);
        level = SceneManager.GetActiveScene().name;
        SceneManager.LoadScene("DeathScene");
        SwitchInput.ClearCache();
        GravityBehavior.ResetAll();
    }
	
	// Update is called once per frame
	void Update () {
		if(Input.GetKeyDown(reviveKey))
        {
            SceneManager.LoadScene(level);
        }
        else if(Input.GetKeyDown(resetKey))
        {
            SceneManager.LoadScene(startingLevel);
        }
        else
        {
            return;
        }
        Destroy(this.gameObject);
	}
}