using System;
using System.Collections.Generic;
using System.IO;
using UnityEditor;
using UnityEngine;

public class CreateFolders : EditorWindow
{
    private static string projectname = "ProjectName";

    [MenuItem("Assets/ Create Defaults Folders")]
    private static void SetUpFolders()
    {
        CreateFolders window = ScriptableObject.CreateInstance<CreateFolders>();
        window.position = new Rect(Screen.width/2, Screen.height/2, 400, 150);      
        window.ShowPopup();
    }

    private static void CreateAllFolders()
    {
        List<string> folders = new List<string>
        {
            "Animations",
            "Audio",
            "Materials",
            "Prefabs",
            "Scripts",
            "Scenes",
            "Shaders",
            "Textures",
            "UI"
        };
        foreach (string folder in folders)
        {
            string path = "Assets/" + projectname + "/" + folder;
            if (!Directory.Exists("Assets/" + folder))
            {
                
                Directory.CreateDirectory(path);
                CreateMetaFile(path);
            }
        }

        List<string> uiFolders = new List<string>
        {
            "Assets",
            "Fonts",
            "Icon"
        };

        foreach (string subfolder in uiFolders)
        {
            string path = "Assets/" + projectname + "/UI/" + subfolder;
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
                CreateMetaFile(path);
            }
        }
        
        AssetDatabase.Refresh();
    }
    
    private static void CreateMetaFile(string folderPath)
    {
        string metaFilePath = Path.Combine(folderPath, ".meta");
        if (!File.Exists(metaFilePath))
        {
            File.WriteAllText(metaFilePath, "# Meta file for Git tracking\n");
        }
    }

    private void OnGUI()
    {
        EditorGUILayout.LabelField("Insert the project name used as the root folder", projectname);
        projectname = EditorGUILayout.TextField("Project Name", projectname);
        this.Repaint();
        GUILayout.Space(70);
        if (GUILayout.Button("Generate!"))
        {
            CreateAllFolders();
            this.Close();
        }
    }
}