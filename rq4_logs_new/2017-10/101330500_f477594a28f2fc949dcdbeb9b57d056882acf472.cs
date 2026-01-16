using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.VR;
using UnityEngine.SceneManagement;

public class MenuController : MonoBehaviour {

    public void SetParticipantID(string id)
    {
        GameControl.Instance.participantID = id;
    }

    public void SetDifficulty(int diff)
    {
        GameControl.Instance.difficulty = diff;
    }

    public void StartTrials()
    {
        SceneManager.LoadScene("Calibrate");
    }

	// Use this for initialization
	void Start () {
        VRSettings.enabled = false;
    }
}