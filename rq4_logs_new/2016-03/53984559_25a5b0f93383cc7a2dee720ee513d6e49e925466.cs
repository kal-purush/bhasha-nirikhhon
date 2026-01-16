using UnityEngine;
using System.Collections;

[RequireComponent(typeof(CircleCollider2D))]
public class EndPointScript : MonoBehaviour {

	private GameObject StartingPuzzlePoint;
	private GameObject PuzzlePointsInLevel;
	private InteractionWithPlayer pointEnabled;
	public GameObject levelManager;
	private LevelManager levelManagerScript;
	
	void Start () {
		//Finds the starting object and its script
		StartingPuzzlePoint = GameObject.Find("StartingPuzzlePoint");
		PuzzlePointsInLevel = GameObject.Find("PuzzlePointsInLevel");
		pointEnabled = PuzzlePointsInLevel.GetComponentInChildren<InteractionWithPlayer>();
		levelManagerScript = levelManager.GetComponent<LevelManager>();
	}
	
	//If StartingPuzzlePoint hits this object and particle system on all other puzzlepoints = play then access next level script and load next level
	void OnTriggerEnter2D (Collider2D other) {
		//TODO FOR SOME REASON IT ONLY CHECKS IF THE FIRST CHILD PUZZLEPOINT isACTIVE FIX TO CHECK FOR ALL USE AN ARRAY
		if (pointEnabled.isActive == true) {
			levelManagerScript.LoadNextLevel();
		}
	}
	
}