using UnityEngine;
using System.Collections;

public class CoolBlocks : MonoBehaviour {

	public int numBlocks = 100;
	public float outerSceneSize = 5.0f;
	public float innerSceneSize = 2.0f;
	//public int scale = 3;

	// Use this for initialization
	void Start () {
		Random.seed = (int) Time.time;
		for (int a=0; a < numBlocks; ++a) {
			makeBlock();
		}
	}

	float randSign () {return (Random.value >= .5) ? 1.0f : -1.0f;}

	void makeBlock () {
		// Randomly generate size and location
		float xLoc = Random.value * outerSceneSize;//(randSign()*(innerSceneSize + Random.value*(outerSceneSize-innerSceneSize)));
		float yLoc = Random.value * outerSceneSize; //(randSign()*(innerSceneSize + Random.value*(outerSceneSize-innerSceneSize)));
		float zLoc = Random.value * outerSceneSize;//(randSign()*(innerSceneSize + Random.value*(outerSceneSize-innerSceneSize)));

		//float scaler = Random.value * Mathf.Pow (2.0f, randSign()*scale);
		// Create block
		GameObject myCube = GameObject.CreatePrimitive(PrimitiveType.Cube);
		myCube.GetComponent<Renderer>().material.color = new Color (Random.value, Random.value, Random.value,1);
		if (Mathf.Abs (xLoc) >= innerSceneSize && Mathf.Abs (yLoc) >= innerSceneSize && Mathf.Abs (zLoc) >= innerSceneSize) {
			xLoc -= 40;
			yLoc -= 30;
			zLoc -= 30;
			Vector3 spawnLocation = new Vector3(xLoc, yLoc, zLoc);
			Instantiate (myCube, spawnLocation, Quaternion.identity);
		}
		//myCube.transform.localScale = new Vector3(scaler, scaler, scaler);
	}
}