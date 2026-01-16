using UnityEngine;
using System.Collections;

public class VMELoadScene : MonoBehaviour {

	public string SceneName;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	void LoadScene()
	{
		Application.LoadLevel(SceneName);
	}
}