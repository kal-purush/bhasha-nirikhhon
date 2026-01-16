using UnityEngine;
using UnityEditor;
using System.Collections;

public class InjuryAnimationCurve : EditorWindow {
	private static AnimationCurve linear = AnimationCurve.Linear (10, 0, 0, 10);
	public AnimationCurve curveX = linear;
	public Transform transform;

	[MenuItem("Create Curve for Object")]
	public static void Init()
	{
		InjuryAnimationCurve w = (InjuryAnimationCurve) GetWindow (typeof(InjuryAnimationCurve));
		w.Show ();
	}

	public void OnGUI() {
		curveX = EditorGUILayout.CurveField ("X Axis Animation", curveX);
		if (GUILayout.Button ("Generate Curve")) {
			AddCurveToSelectedObject();
		}
	}

	public void AddCurveToSelectedObject() {
		if (Selection.activeGameObject) {
			InjuryAnimation comp = Selection.activeGameObject.AddComponent<InjuryAnimation> ();
			comp.SetCurves (curveX);
		} else {
			Debug.LogError ("No Game Object selected for adding animation curve");
		}
	}
}