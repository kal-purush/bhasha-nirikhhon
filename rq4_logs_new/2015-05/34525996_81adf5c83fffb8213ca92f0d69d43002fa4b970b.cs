using UnityEngine;
using System.Collections;

public class FPS_Script : MonoBehaviour 
{
	public GUIText guiTimer;
	public float refreshRate;
	private float nextRefresh = 0;

	void Start () 
	{
	
	}
	

	void Update () 
	{
		if(Time.time > nextRefresh)
		{
			guiTimer.text = ((int)(1 / Time.deltaTime)).ToString ();
			nextRefresh = Time.time + refreshRate;
		}
	}
}