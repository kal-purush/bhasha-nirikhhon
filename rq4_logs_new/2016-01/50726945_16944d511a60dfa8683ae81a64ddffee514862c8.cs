using UnityEngine;
using UnityEditor;
using System.Collections;

[CustomEditor(typeof(MazePhanton))]
public class MazePhantomInspector : Editor {

	MazePhanton phathon;
	int pathPointId = -1;

	// Use this for initialization
	void Awake () {
		phathon = (MazePhanton) target;
	}
	
	void OnSceneGUI () {
		for (int i = 0; i < phathon.path.Length; i++) {
			if (pathPointId == i) {
				if (Handles.Button(phathon.path[i], Quaternion.identity, 0.2f, 0.2f, Handles.DotCap)) {
					pathPointId = i;
				}
			}else {
				phathon.path[i] = Handles.PositionHandle(phathon.path[i], Quaternion.identity);
			}
		}
	}
}