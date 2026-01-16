using System.IO;
using UnityEditor;
using UnityEngine;

public class DBControllerMenu : MonoBehaviour
{
    private const string FILE_NAME_PATTERN = "Profile_*.txt";

    [MenuItem("DBController/Goto Database Folder",false,0)]
    public static void GotoDatabaseFolder()
    {
        string folderPath = Application.persistentDataPath;
        if (Directory.Exists(folderPath))
        {
            EditorUtility.RevealInFinder(folderPath);
            Debug.Log("Opened database folder");
        }
        else
        {
            Debug.LogError("Database folder not found");
        }
    }
    
    [MenuItem("DBController/Clear Specific Profile")]
    public static void ShowClearSpecificProfileWindow()
    {
        ClearSpecificProfileWindow.ShowWindow();
    }

    [MenuItem("DBController/Delete All Database")]
    public static void DeleteDatabaseFile()
    {
        if (EditorUtility.DisplayDialog("Confirm Delete", "Do you really want to delete all the database files?", "Yes", "No"))
        {
            string[] files = Directory.GetFiles(Application.persistentDataPath, FILE_NAME_PATTERN);
            if (files.Length > 0)
            {
                foreach (string file in files)
                {
                    File.Delete(file);
                }
                Debug.Log("Deleted all database files");
            }
            else
            {
                Debug.LogError("No database files found");
            }
        }
    }
}

public class ClearSpecificProfileWindow : EditorWindow
{
    private string profileIndex = "";

    public static void ShowWindow()
    {
        GetWindow<ClearSpecificProfileWindow>("Clear Specific Profile");
    }

    private void OnGUI()
    {
        GUILayout.Label("Enter the profile index to clear:", EditorStyles.boldLabel);
        profileIndex = EditorGUILayout.TextField("Profile Index", profileIndex);

        if (GUILayout.Button("Clear Profile"))
        {
            if (int.TryParse(profileIndex, out int index))
            {
                string fileName = string.Format("Profile_{0}.txt", index);
                string filePath = Path.Combine(Application.persistentDataPath, fileName);
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                    Debug.Log($"Deleted Profile_{index}");
                }
                else
                {
                    Debug.LogError($"Profile_{index} not found");
                }
            }
            else
            {
                Debug.LogError("Invalid profile index");
            }
        }
    }
}