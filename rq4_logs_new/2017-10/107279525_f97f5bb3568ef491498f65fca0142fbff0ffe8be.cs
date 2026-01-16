using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class Scene : MonoBehaviour {

	public Text countText;
	public Text winText;
	public string nextscene;

	void Start () {
		winText.text = "";
		
	}

	void Update () {
		int enemynum =  GameObject.FindGameObjectsWithTag("Enemy").Length;
		int playernum = GameObject.FindGameObjectsWithTag("Player").Length;
		countText.text = "Enemy: " + enemynum.ToString();
		if (enemynum <= 0) {
			winText.text = "YOU WIN";
			Invoke("goNextScene", 3.5f);
		}
		if (playernum <= 0) {
			winText.text = "YOU LOSE";
			Invoke("goNextScene", 3.5f);
		}
	}

	void goNextScene() {
		SceneManager.LoadScene(nextscene);
	}
}