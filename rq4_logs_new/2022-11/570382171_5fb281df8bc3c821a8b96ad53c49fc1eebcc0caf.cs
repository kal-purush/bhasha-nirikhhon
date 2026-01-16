using System.Collections;
using System.Collections.Generic;
using UnityEngine;

// This class is used for storing building information and settings

[ExecuteInEditMode]
[RequireComponent(typeof(MeshFilter), typeof(MeshRenderer))]
public class BuildingCreator : MonoBehaviour {

    // Singleton functionality
    // https://gamedevbeginner.com/singletons-in-unity-the-right-way/
    public static BuildingCreator main { get; private set; }
    void OnEnable() {
        if (main == null) {
            main = this;
        }
    }

    // List of buildings
    [HideInInspector]
    public List<Building> buildings = new List<Building>();

    // Settings
    [Range(0.5f, 3f)]
    public float handleRadius = 0.5f;
    [HideInInspector]
    public bool showBuildingsList = false;
    [HideInInspector]
    public bool showVerticalLines = true;
    [HideInInspector]
    public bool showSelectionInfo = false; 

}

// Building information and settings

[System.Serializable]
public class Building {
    // List of points for the building
    public List<Vector3> points = new List<Vector3>();
    
    // Mesh of the building
    public Mesh mesh;
    // Material of the building
    public Material buildingMaterial;
    // Windows and doors
    public GameObject window;
    public GameObject door;

    // Settings
    public bool inverted = false;
    public float edgeOffset;
    public float gap;
    public float height = 5;

}

// This class stores selection information

public static class SelectionInfo {

    // Drag
    public static bool pointIsBeingDragged = false;
    public static Vector3 positionAtDragStart;

    // Selection
    public static int buildingIndex;

    // Mouse Over
    public static int mouseOverBuildingIndex = -1;
    public static int mouseOverPointIndex = -1;
    public static int mouseOverLineIndex = -1;
    public static bool mouseIsOverPoint = false;
    public static bool mouseIsOverLine = false;

}