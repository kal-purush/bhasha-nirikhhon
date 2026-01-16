using UnityEngine;
using System.Collections;

public class ClickTestScript : MonoBehaviour {

	public void ClickToTest()
	{
		Debug.Log(Application.persistentDataPath);
		foreach(string name in LevelIOManager.GetBuiltInLevelNames(false))
		{
			Debug.Log("Builtin: " + name);
		}
		foreach(string name in LevelIOManager.GetBuiltInLevelNames(true))
		{
			Debug.Log("Custom: " + name);
		}
	}
}