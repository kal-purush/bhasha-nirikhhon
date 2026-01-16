using UnityEngine;
using System.Collections;

public class MuteButton : MonoBehaviour {

    public AudioSource source;
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        if (Input.GetKeyDown(KeyCode.M))
        {
            if (source.mute) source.mute = false;
            else source.mute = true;
        }
	}
}