using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor;

[CustomEditor(typeof(Actuator))]
public class ActuatorInspector : Editor {

    public override void OnInspectorGUI()
    {
        Actuator actuatorScript = (Actuator) target;
        actuatorScript.ID = EditorGUILayout.IntField("ID", actuatorScript.ID);
        EditorGUILayout.LabelField("State", actuatorScript.getState().ToString());
    }
    
}