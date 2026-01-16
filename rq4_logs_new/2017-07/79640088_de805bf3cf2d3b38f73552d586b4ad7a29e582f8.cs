using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor;

[CustomEditor(typeof(TerrainGenerator))]
public class TerrainGeneratorEditor : Editor {
    public Shape shape;
    public Platform plat;

    public override void OnInspectorGUI()
    {
        TerrainGenerator terrain = (TerrainGenerator)target;

        shape = (TerrainGeneratorEditor.Shape) terrain.shape;
        shape = (Shape)EditorGUILayout.EnumPopup("Shape", shape);
        plat = (TerrainGeneratorEditor.Platform) terrain.plat;
        plat = (Platform)EditorGUILayout.EnumPopup("Platform type", plat);

        switch (shape) {
            case Shape.Plane:
                terrain.shape = TerrainGenerator.Shape.Plane;
                break;
            case Shape.Sphere:
                terrain.shape = TerrainGenerator.Shape.Sphere;
                break;
        }

        switch (plat) {
            case Platform.Square:
                terrain.Square = (GameObject)EditorGUILayout.ObjectField("Square", terrain.Square, typeof(GameObject), true);
                terrain.SquareWidth = EditorGUILayout.FloatField("Square Width", terrain.SquareWidth);
                terrain.plat = TerrainGenerator.Platform.Square;
                break;
            case Platform.Spike:
                terrain.Spike = (GameObject)EditorGUILayout.ObjectField("Spike", terrain.Spike, typeof(GameObject), true);
                terrain.invertSpikes = EditorGUILayout.Toggle("Invert Spikes", terrain.invertSpikes);
                terrain.plat = TerrainGenerator.Platform.Spike;
                break;
        }

        if (GUILayout.Button("Build"))
        {
            terrain.Generate();
        }

    }

    public enum Shape {
        Plane = 0,
        Sphere = 1,
    }

    public enum Platform {
        Square = 0,
        Spike = 1,
    }

}