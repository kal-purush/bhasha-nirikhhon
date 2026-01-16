using UnityEditor;
using UnityEngine;

[CustomEditor(typeof(TreeSwitcher))]
public class TreeSwitcherEditor : Editor
{
    public override void OnInspectorGUI()
    {
        DrawDefaultInspector();

        TreeSwitcher treeSwitcher = (TreeSwitcher)target;

        if (GUILayout.Button("Toggle Trees"))
        {
            treeSwitcher.ToggleTrees();
        }
    }
}