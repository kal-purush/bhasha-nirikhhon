using UnityEngine;
using System;
using System.Collections;

public class PaceTimer : MonoBehaviour {

	private static BikeController bikeController;
	public TextMesh paceDisplay;
	static float trackLength;
	static float targetPaceIncrement;
	static float targetPace = 0f;
	static float actualPace;

	// Use this for initialization
	void Start () {

		if (ExerciseSettings.durationIsDistance == false) {
			trackLength = ((ExerciseSettings.time / 60) * ExerciseSettings.targetSpeed * 1000) - (((ExerciseSettings.time / 60) * ExerciseSettings.targetSpeed * 1000) % 10);

			//Disable script if track isn't long enough.
			if (trackLength <= 100) {
				GetComponent<PaceTimer> ().enabled = false;
			}
		}

		PaceTimer.bikeController = bikeController;

		paceDisplay = gameObject.GetComponent<TextMesh> ();
		targetPaceIncrement = (trackLength / (ExerciseSettings.targetSpeed * 1000)) * 3600;

	}
	
	// Update is called once per frame
	void Update () {
		if (bikeController.TimerStarted) {
			TimeSpan currentTime;
			currentTime = DateTime.Now - bikeController.ReferenceTime;

			if (bikeController.DistanceTravelled % 100 == 0) {
				targetPace += targetPaceIncrement;
				actualPace = targetPace
				- (((Mathf.Floor ((float)currentTime.TotalHours)) * 3600)
				+ ((Mathf.Floor ((float)currentTime.Minutes)) * 60)
				+ (Mathf.Floor ((float)currentTime.Seconds))
				+ ((Mathf.Floor ((float)currentTime.Milliseconds)) * 10));

				if (actualPace >= 0f){
					paceDisplay.text = "+" + actualPace;
				}else{
					paceDisplay.text = "" + actualPace;
				}
				paceDisplay.GetComponent<Renderer> ().enabled = true;
				StartCoroutine (HideRenderer ());
			}
		}
	}

	IEnumerator HideRenderer(){
		yield return new WaitForSeconds (3.0f);
		paceDisplay.GetComponent<Renderer> ().enabled = false;
	}
}