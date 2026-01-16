using UnityEngine;
using UnityEditor;
using ReupVirtualTwin.behaviours;

namespace ReupVirtualTwin.editor
{
    [CustomEditor(typeof(SetUpBuilding))]
    public class SetUpBuildingEditor : Editor
    {
        public override void OnInspectorGUI()
        {
            base.OnInspectorGUI();

            SetUpBuilding setUpBuilding = (SetUpBuilding)target;

            if (GUILayout.Button("Add Ids to objects"))
            {
                AssignIds.AssignToTree(setUpBuilding.building);
            }
        }
    }
}