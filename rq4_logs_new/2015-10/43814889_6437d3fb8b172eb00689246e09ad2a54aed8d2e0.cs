using UnityEngine;
using UnityEditor;
using System.Collections;

class EditorTools : EditorWindow
{
	private GameObject _replacementGO;
	private bool runForNonPrefabs;
	private bool keepOldNames;
	
	[MenuItem("Window/Editor Tools")]
	public static void ShowWindow()
	{
		EditorWindow.GetWindow(typeof(EditorTools));
	}
	
	void OnGUI()
	{
		ReplaceSelectionEditor();
	}
	
	void ReplaceSelectionEditor()
	{
		PrefabType prefabType;
		
		EditorGUILayout.LabelField("Replace Selection", EditorStyles.boldLabel);
		EditorGUILayout.LabelField("Replace selection with GameObject, maintaining transform(s)", EditorStyles.label);
		_replacementGO = EditorGUILayout.ObjectField(_replacementGO, typeof(GameObject), true) as GameObject;
		runForNonPrefabs = EditorGUILayout.Toggle("Allow non-prefab", runForNonPrefabs);
		keepOldNames = EditorGUILayout.Toggle("Maintain name(s)", keepOldNames);
		
		if (_replacementGO == null)
			return;
		
		prefabType = PrefabUtility.GetPrefabType(_replacementGO);
		
		if (prefabType != PrefabType.Prefab && !runForNonPrefabs)
		{
			EditorGUILayout.HelpBox("Object is not a prefab", MessageType.Warning);
			return;
		}
		
		if (GUILayout.Button("Replace"))
			ReplaceSelectionWith(_replacementGO);
		
	}
	
	void ReplaceSelectionWith(Object replacement)
	{
		GameObject[] selectedGOs;
		
		selectedGOs = Selection.gameObjects;
		
		foreach(GameObject oldGO in selectedGOs)
		{
			GameObject newGO;
			if(runForNonPrefabs)
			{
				newGO = Instantiate(replacement) as GameObject;
			} else
			{
				newGO = PrefabUtility.InstantiatePrefab(replacement) as GameObject;
			}
			
			newGO.transform.position = oldGO.transform.position;
			newGO.transform.rotation = oldGO.transform.rotation;
			newGO.transform.localScale = oldGO.transform.localScale;
			newGO.transform.SetParent(oldGO.transform.parent);
			newGO.transform.SetSiblingIndex(oldGO.transform.GetSiblingIndex());
			
			if (keepOldNames)
				newGO.name = oldGO.name;
			
			
			DestroyImmediate(oldGO);
		}
	}
}