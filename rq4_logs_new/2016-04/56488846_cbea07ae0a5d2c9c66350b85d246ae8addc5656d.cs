using UnityEngine;  
using UnityEditor;  
using UnityEditorInternal;

[CustomEditor(typeof(WaveManager))]
public class WaveManagerEditor : Editor {  

	private ReorderableList list;
	
	private void OnEnable() {
		list = new ReorderableList(serializedObject, 
		                           serializedObject.FindProperty("Waves"), 
		                           true, true, true, true);

		list.drawElementCallback =  
		(Rect rect, int index, bool isActive, bool isFocused) => {
			var element = list.serializedProperty.GetArrayElementAtIndex(index);
			rect.y += 2;
			EditorGUI.PropertyField(
				new Rect(rect.x, rect.y, 60, EditorGUIUtility.singleLineHeight),
				element.FindPropertyRelative("Type"), GUIContent.none);
			EditorGUI.PropertyField(
				new Rect(rect.x + 60, rect.y, rect.width - 60 - 30, EditorGUIUtility.singleLineHeight),
				element.FindPropertyRelative("Prefab"), GUIContent.none);
			EditorGUI.PropertyField(
				new Rect(rect.x + rect.width - 30, rect.y, 30, EditorGUIUtility.singleLineHeight),
				element.FindPropertyRelative("Count"), GUIContent.none);
		};

		list.drawHeaderCallback = (Rect rect) => {  
			EditorGUI.LabelField(rect, "Wave Definitions");
		};
	}
	
	public override void OnInspectorGUI() {
		DrawDefaultInspector ();
		GUILayout.Space (10);
		serializedObject.Update();
		list.DoLayoutList();
		serializedObject.ApplyModifiedProperties();
		GUILayout.Space (10);
	}
}