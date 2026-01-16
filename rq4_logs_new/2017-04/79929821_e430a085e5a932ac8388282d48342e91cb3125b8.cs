using System.Collections;
using UnityEngine.SceneManagement;
using UnityEngine;

public class MoveScene : MonoBehaviour {

	public string loadLevel;

	void onTriggerEnter(Collider other){
		print ("is called");


	}

	void OnCollisionEnter(Collision collision) {
		print ("Collision called");
		//if(collision.CompareTag("Player")){
			SceneManager.LoadScene(loadLevel);

		//}
	}

}