using UnityEngine;
using UnityEditor;
using SplashEdit.RuntimeCode;
using System.Linq;

namespace SplashEdit.EditorCode
{
    [CustomEditor(typeof(PSXNavMesh))]
    public class PSXNavMeshEditor : Editor
    {
        public override void OnInspectorGUI()
        {
            DrawDefaultInspector();

            PSXNavMesh comp = (PSXNavMesh)target;
            if (GUILayout.Button("Create preview"))
            {
                PSXSceneExporter exporter = FindObjectsByType<PSXSceneExporter>(FindObjectsSortMode.None).FirstOrDefault();
                if(exporter != null)
                {
                    comp.CreateNavmesh(exporter.GTEScaling);
                }
                else
                {
                    Debug.LogError("No PSXSceneExporter found in the scene. We can't pull the GTE scaling from the exporter.");
                }                
            }

        }
    }
}