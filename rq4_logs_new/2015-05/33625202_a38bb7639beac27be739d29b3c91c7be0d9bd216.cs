using UnityEngine;
using System.Collections;

public class MainMenu : MonoBehaviour {
	public int nextlevel = 2;
	
	// Update is called once per frame
	void Update () {
		if( Input.anyKeyDown ) {
			Application.LoadLevel( nextlevel );
		}
	}
}