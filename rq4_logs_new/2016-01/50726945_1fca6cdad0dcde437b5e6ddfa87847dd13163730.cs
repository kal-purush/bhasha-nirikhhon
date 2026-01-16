using UnityEngine;
using UnityEditor;
using System.Collections;

[CustomEditor(typeof(MazePhantonPath))]
public class MazePhantonPathInspector : Editor {

	MazePhantonPath phathon;
	int pathPointId = -1;

	// Use this for initialization
	void Awake () {
		
	}
	
	void OnSceneGUI () {
		if (!phathon)
			phathon = (MazePhantonPath) target;
		
		for (int i = 0; i < phathon.path.Length; i++) {
			if (pathPointId != i) {
				if (Handles.Button(phathon.path[i], Quaternion.identity, 0.05f, 0.05f, Handles.DotCap)) {
					pathPointId = i;
				}
			}else {
				phathon.path[i] = Snap(Handles.PositionHandle(phathon.path[i], Quaternion.identity));
			}
			Handles.Label (phathon.path [i] + Vector3.up * 0.1f, phathon.wait [i].ToString ());
		}

		if (phathon.path.Length < 2)
			return;
		
		Vector3 v0 = phathon.path [0];
		for (int i = 1; i < phathon.path.Length; i++) {
			Vector3 v1 = phathon.path [i];
			Handles.DrawLine (v0, v1);
			v0 = v1;
		}
	}

	Vector3 Snap (Vector3 v) {
		v.x = ((int)(v.x / 0.5f)) * 0.5f;
		v.y = ((int)(v.y / 0.5f)) * 0.5f;
		v.z = ((int)(v.z / 0.5f)) * 0.5f;
		return v;
	}
}