using UnityEngine;
using System.Collections;
using UnityEditor;

/*
 * @author Mike Dobson
 * */

[CustomEditor(typeof(ScriptEngine))]
public class EngineEditor :  Editor
{
	
	ScriptEngine engineScript;

	void Awake()
	{
		//engineScript = (ScriptEngine)engineScript;
	}

	public override void OnInspectorGUI()
	{
		//required things for arrays
		serializedObject.Update ();

		//-------------------------------
		//Place your custom editor stuffs
		//serializedObject.waypoints
		SerializedProperty movementsArray = serializedObject.FindProperty ("movements");
		SerializedProperty effectsArray = serializedObject.FindProperty ("effects");
		SerializedProperty facingsArray = serializedObject.FindProperty ("facings");

		EditorGUILayout.PropertyField(movementsArray);

		if (movementsArray.isExpanded)
        {
            //EditorGUILayout.PropertyField(waypointsArray.arraySize)
			EditorGUILayout.PropertyField(movementsArray.FindPropertyRelative("Array.size"));
            EditorGUI.indentLevel++;
			for (int i = 0; i < movementsArray.arraySize; i++)
            {
				SerializedProperty showInEditor = movementsArray.GetArrayElementAtIndex(i).FindPropertyRelative("showInEditor");

                showInEditor.boolValue = EditorGUILayout.Foldout(showInEditor.boolValue, "Movement " + ( i + 1 ));

                if (showInEditor.boolValue)
                {
                    //EditorGUILayout.LabelField("Movement " + (i + 1));
					EditorGUILayout.PropertyField(movementsArray.GetArrayElementAtIndex(i));
                }
            }
            EditorGUI.indentLevel--;
        }

		EditorGUILayout.PropertyField(effectsArray);
		
		if (effectsArray.isExpanded)
		{
			//EditorGUILayout.PropertyField(waypointsArray.arraySize)
			EditorGUILayout.PropertyField(effectsArray.FindPropertyRelative("Array.size"));
			EditorGUI.indentLevel++;
			for (int i = 0; i < effectsArray.arraySize; i++)
			{
				SerializedProperty showInEditor = effectsArray.GetArrayElementAtIndex(i).FindPropertyRelative("showInEditor");
				
				showInEditor.boolValue = EditorGUILayout.Foldout(showInEditor.boolValue, "Effect " + ( i + 1 ));
				
				if (showInEditor.boolValue)
				{
					//EditorGUILayout.LabelField("Movement " + (i + 1));
					EditorGUILayout.PropertyField(effectsArray.GetArrayElementAtIndex(i));
				}
			}
			EditorGUI.indentLevel--;
		}

		EditorGUILayout.PropertyField(facingsArray);
		
		if (facingsArray.isExpanded)
		{
			//EditorGUILayout.PropertyField(waypointsArray.arraySize)
			EditorGUILayout.PropertyField(facingsArray.FindPropertyRelative("Array.size"));
			EditorGUI.indentLevel++;
			for (int i = 0; i < facingsArray.arraySize; i++)
			{
				SerializedProperty showInEditor = facingsArray.GetArrayElementAtIndex(i).FindPropertyRelative("showInEditor");
				
				showInEditor.boolValue = EditorGUILayout.Foldout(showInEditor.boolValue, "Facings " + ( i + 1 ));
				
				if (showInEditor.boolValue)
				{
					//EditorGUILayout.LabelField("Movement " + (i + 1));
					EditorGUILayout.PropertyField(facingsArray.GetArrayElementAtIndex(i));
				}
			}
			EditorGUI.indentLevel--;
		}
        //--------------------------------


        //required things for arrays
        serializedObject.ApplyModifiedProperties();
	}
}