using UnityEditor;
using UnityEngine;

[CustomEditor(typeof(MapGeneraator))]
public class MapGeneratorEditor : Editor
{
    public override void OnInspectorGUI()
    {
        DrawDefaultInspector();

        MapGeneraator myScript = (MapGeneraator)target;
        if (GUILayout.Button("Generate map grid"))
        {
            myScript.GenerateMap();
        }

        if (GUILayout.Button("Save map grid"))
        {
            myScript.SaveToTextureMap();
        }

        if (GUILayout.Button("Load map grid"))
        {
            myScript.LoadFromPng();
        }

        if (GUILayout.Button("Generate mesh"))
        {
            myScript.GenerateMesh();
        }
    }
}