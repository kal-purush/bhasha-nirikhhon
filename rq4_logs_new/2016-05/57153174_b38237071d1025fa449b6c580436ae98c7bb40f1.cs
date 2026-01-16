using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Vuforia;

public class LiveTrackablesScript : MonoBehaviour {

    public GameObject arCamera;
    public GameObject gameCamera;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        gameCamera.SetActive(false);
        arCamera.SetActive(true);

        StateManager sm = TrackerManager.Instance.GetStateManager();

        IEnumerable<TrackableBehaviour> activeTrackables = sm.GetActiveTrackableBehaviours();

        foreach (TrackableBehaviour tb in activeTrackables)
        {
            if (tb.TrackableName == "stones")
            {
                Debug.Log("Found ground target");
                gameCamera.SetActive(true);
                arCamera.SetActive(false);
            } else if (tb.TrackableName == "pavement")
            {
                Debug.Log("ground target lost");
                gameCamera.SetActive(false);
                arCamera.SetActive(true);
            }
        }
        if (!activeTrackables.Any())
        {
            Debug.Log("ground target lost");
            gameCamera.SetActive(false);
            arCamera.SetActive(true);
        }
    }
}