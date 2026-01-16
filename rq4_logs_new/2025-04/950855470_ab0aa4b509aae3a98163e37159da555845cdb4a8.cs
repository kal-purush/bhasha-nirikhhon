using UnityEngine;
using UnityEditor;
using UnityEditor.SceneManagement;

[CustomEditor(typeof(Board))]
public class BoardEditor : Editor
{
    public override void OnInspectorGUI()
    {
        DrawDefaultInspector();

        Board board = (Board)target;

        // Add a button to manually assign IDs
        if (GUILayout.Button("Assign Tile IDs"))
        {
            AssignTileIDs(board);
        }
    }

    private void AssignTileIDs(Board board)
    {
        // Get the tile container reference via SerializedProperty
        SerializedProperty tileContainerProp = serializedObject.FindProperty("tileContainer");
        GameObject tileContainer = tileContainerProp.objectReferenceValue as GameObject;

        if (tileContainer == null)
        {
            Debug.LogError("Tile Container is not assigned!");
            return;
        }

        // Get all BoardTile components from children
        BoardTile[] tiles = tileContainer.GetComponentsInChildren<BoardTile>();

        int id = 0;
        foreach (BoardTile tile in tiles)
        {
            tile.setIndex(id++);
            EditorUtility.SetDirty(tile); // Mark the tile as dirty so changes are saved
        }

        Debug.Log($"Assigned IDs to {id} tiles");

        // Save the changes
        EditorUtility.SetDirty(board);
        if (!Application.isPlaying)
        {
            EditorSceneManager.MarkSceneDirty(board.gameObject.scene);
        }
    }
}