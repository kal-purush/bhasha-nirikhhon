using UnityEditor;
using UnityEditor.Callbacks;
using UnityEngine;

public class TestPostBuild : MonoBehaviour {

	[PostProcessBuild]
	public static void OnPostProcessBuild(BuildTarget target, string pathToBuiltProject)
	{
		Debug.Log("Build location: " + pathToBuiltProject);
	}
}