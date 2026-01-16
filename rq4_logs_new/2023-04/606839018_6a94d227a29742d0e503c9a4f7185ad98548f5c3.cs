using UnityEditor;
using UnityEngine;

[CustomEditor(typeof(PropGenerator))]
public class PropGeneratorEditor : Editor
{
    public override void OnInspectorGUI()
    {
        DrawDefaultInspector();

        PropGenerator myScript = (PropGenerator)target;
        if (GUILayout.Button("Generate positions"))
        {
            myScript.GeneratePositions();
        }

        if (GUILayout.Button("Select random positions"))
        {
            myScript.SelectRandomly();
        }

        if (GUILayout.Button("Place at positions"))
        {
            myScript.PlacePrefabs();
        }
    }
}