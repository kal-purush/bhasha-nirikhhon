using System.Globalization;
using UnityEngine;
using UnityEditor;

public class ScriptConfirmationWindow : EditorWindow
{
    private string author;
    private string date;
    private string levelName;

    // Creates a new window that confirms the export, prompts user for author and level name
    public static void Init()
    {
        ScriptConfirmationWindow window = (ScriptConfirmationWindow)EditorWindow.GetWindow(typeof(ScriptConfirmationWindow));
        window.position = new Rect(200, 200, 300, 75);
        window.maxSize = new Vector2(300, 75);
        window.minSize = window.maxSize;
        window.Show();
    }

    void OnGUI()
    {

        // Get the author of the level
        author = EditorGUILayout.TextField("Author ");

        // Get the date as a string
        date = System.DateTime.Today.ToString();

        // Get the level name
        levelName = EditorGUILayout.TextField("Level Name ");

        if (GUILayout.Button("Confirm Export"))
        {
            // Export initiation code goes here
            Debug.Log("Export Confirmed");
        }

    }
}