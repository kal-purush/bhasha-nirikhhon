using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;

public class FallTeleport : MonoBehaviour {

	public bool playerInZone;

	public string levelToLoad;



	// Use this for initialization
	void Start () {
		playerInZone = false;

	}

	// Update is called once per frame
	void Update () {
		if (playerInZone) {
			SceneManager.LoadScene(levelToLoad);
		}

	}

	/// <summary>
	/// Loads the level.
	/// </summary>
	public void LoadLevel(){
		SceneManager.LoadScene(levelToLoad);
	}

	/// <summary>
	/// Sets playInZone true when player is in zone
	/// </summary>
	/// <param name="other">The other.</param>
	void OnTriggerEnter2D(Collider2D other){
		if (other.name == "Player") {
			playerInZone = true;
		}
	}

	/// <summary>
	/// Sets playInZone false when player is not in zone
	/// </summary>
	/// <param name="other">The other.</param>
	void OnTriggerExit2D(Collider2D other){
		if (other.name == "Player") {
			playerInZone = false;
		}
	}
}
