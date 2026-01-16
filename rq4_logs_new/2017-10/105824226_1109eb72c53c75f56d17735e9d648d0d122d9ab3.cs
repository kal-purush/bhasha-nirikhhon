using System.Collections;
using System.Collections.Generic;
using UnityEditor;
using UnityEngine;



// Analysis disable once CheckNamespace
public class AddNPC : ScriptableWizard {
	public Sprite ObjectSprite;
	public string ObjectName = "Object";
	public string ObjectTag = "Default";
	public Color ObjectColor = Color.white;

	[MenuItem ("ObjectManager/Create a NPC %g")]
	static void CreatePickableObject(){
		ScriptableWizard.DisplayWizard<AddNPC> ("Set object as NPC");
	}

	void OnWizardCreate(){
		if (Selection.activeTransform != null) {
			GameObject SelectedObject = Selection.activeTransform.gameObject;
			if (SelectedObject != null) {
				Debug.Log ("FUCK");
				SelectedObject.AddComponent(typeof(PathfindingNPC));
			}
		} else {
			Debug.Log ("SELECT AN ITEM FIRST!!!");
		}
	}
}