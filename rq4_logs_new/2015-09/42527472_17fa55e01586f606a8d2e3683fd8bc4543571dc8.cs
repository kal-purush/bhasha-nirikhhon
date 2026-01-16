using UnityEngine;
using System.Collections;
using UnityEditor;

public class ScriptWaypointWindow : EditorWindow
{
    private Vector2 scrollPos1;
    private Vector2 scrollPos2;
    private Vector2 scrollPos3;
    private SerializedObject serializedObject;

    public static void Init()
    {
        ScriptWaypointWindow window = (ScriptWaypointWindow) EditorWindow.GetWindow(typeof (ScriptWaypointWindow));
        window.position = new Rect(500, 500, 931, 500);
        window.maxSize = new Vector2(931, 500);
        window.minSize = window.maxSize;
        window.Show();
    }

    void OnFocus()
    {
        GameObject player = GameObject.Find("Player");

        serializedObject = new SerializedObject(player);
    }

    void OnLostFocus()
    {
        serializedObject.ApplyModifiedProperties();
    }

    void OnGUI()
    {
        
        EditorGUILayout.BeginHorizontal();

        EditorGUILayout.BeginVertical("box");
        EditorGUILayout.LabelField("Movement Waypoints", EditorStyles.boldLabel);
        scrollPos1 = EditorGUILayout.BeginScrollView(scrollPos1, GUILayout.Width(300), GUILayout.Height(450));
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.EndScrollView();
        EditorGUILayout.EndVertical();

        EditorGUILayout.BeginVertical("box");
        EditorGUILayout.LabelField("Facing Waypoints", EditorStyles.boldLabel);
        scrollPos2 = EditorGUILayout.BeginScrollView(scrollPos2, GUILayout.Width(300), GUILayout.Height(450));
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.EndScrollView();
        EditorGUILayout.EndVertical();

        EditorGUILayout.BeginVertical("box");
        EditorGUILayout.LabelField("Effect Waypoints", EditorStyles.boldLabel);
        scrollPos3 = EditorGUILayout.BeginScrollView(scrollPos3, GUILayout.Width(300), GUILayout.Height(450));
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll"); EditorGUILayout.LabelField("Testing the scroll");
        EditorGUILayout.EndScrollView();
        EditorGUILayout.EndVertical();

        EditorGUILayout.EndHorizontal();
        EditorGUILayout.BeginHorizontal();
        EditorGUILayout.LabelField("End of waypoint boxes");
        if (GUILayout.Button("Export Data", "miniButtonRight"))
        {
            Debug.Log("Export data button pushed");
        }
        EditorGUILayout.EndHorizontal();
        /*
        Rect FacingAreaRect = new Rect(position.width / 3, 50, position.width / 3, position.height - 50);
        Rect MovementAreaRect = new Rect(0, 50, position.width / 3, position.height - 50);
        Rect EffectAreaRect = new Rect((position.width / 3) * 2, 50, position.width / 3, position.height - 50);

        EditorGUI.BeginProperty(MovementAreaRect, GUIContent.none, SerializedProperty moveScript);

        GUILayout.Label("Movement Waypoints", EditorStyles.boldLabel);

        EditorGUI.EndProperty();

        EditorGUI.BeginProperty(FacingAreaRect, GUIContent.none);

        GUILayout.Label("Facing Waypoints", EditorStyles.boldLabel);

        EditorGUI.EndProperty();

        EditorGUI.BeginProperty(EffectAreaRect, GUIContent.none);

        GUILayout.Label("Effect Waypoints", EditorStyles.boldLabel);

        EditorGUI.EndProperty();
        */
    }

}