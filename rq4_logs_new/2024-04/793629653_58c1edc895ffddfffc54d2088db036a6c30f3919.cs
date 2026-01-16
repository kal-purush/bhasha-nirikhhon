using Game._Scripts.Runtime.Tools;
using UnityEditor;
using UnityEngine;

namespace Game._Scripts.Editor
{
    public class CustomToolsMenu
    {
        [MenuItem("Custom Tools/Load CSV Data")]
        public static void LoadCsvData()
        {
            var path = EditorUtility.OpenFilePanel("Load CSV Data", "", "csv");

            if (path.Length != 0)
            {
                var csvData = System.IO.File.ReadAllText(path);
                CsvLoaderTool.LoadCsv(csvData);
            }
            else
            {
                Debug.LogError("Invalid file path or no file selected.");
            }
        }
    }
}