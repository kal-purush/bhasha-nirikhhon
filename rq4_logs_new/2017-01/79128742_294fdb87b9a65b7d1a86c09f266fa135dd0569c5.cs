using UnityEngine;
using System.Collections;

public abstract class DDOLSingleton<T> : MonoBehaviour where T: Component
{
	protected static T _instance = null;

	public static T Instance
	{
		get{ 
			if (null == _instance) {
				GameObject go = GameObject.Find ("DDOLGameObject");
				if (null == go) {
					go = new GameObject ("DDOLGameObject");
					DontDestroyOnLoad (go); //will not destory on load scene
				}
				_instance = go.AddComponent<T> ();
			}
			return _instance;
		}

	}

	private void OnApplicationQuit()
	{
		_instance = null;
	}
	// Use this for initialization
	void Start ()
	{
	
	}
	
	// Update is called once per frame
	void Update ()
	{
	
	}
}
