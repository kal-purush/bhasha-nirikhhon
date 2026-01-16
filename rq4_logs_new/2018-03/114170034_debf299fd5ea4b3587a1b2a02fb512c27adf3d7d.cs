using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor;

[CustomEditor(typeof(CinematicCameraMovement))]
public class CinematicCameraMovementEditor : Editor {

    private bool firstRun = true;

    AnimationCurve easeIn = AnimationCurve.Linear(0, 0, 1, 1);
    AnimationCurve easeOut = AnimationCurve.Linear(0, 0, 1, 1);
    AnimationCurve quadratic = AnimationCurve.Linear(0, 0, 1, 1);
    AnimationCurve smoothStep = AnimationCurve.Linear(0, 0, 1, 1);
    AnimationCurve smootherStep = AnimationCurve.Linear(0, 0, 1, 1);
    AnimationCurve linear = AnimationCurve.Linear(0, 0, 1, 1);

    public override void OnInspectorGUI()
    {
        if (firstRun)
        {
            // Ease Out
            Keyframe[] ks1 = new Keyframe[50];
            for (int i = 0; i < 50; i++)
            {
                float t = ((float)i / 50);
                ks1[i] = new Keyframe(t, Mathf.Sin(t * Mathf.PI * 0.5f));
            }
            easeIn.keys = ks1;

            // Ease In
            Keyframe[] ks2 = new Keyframe[50];
            for (int i = 0; i < 50; i++)
            {
                float t = ((float)i / 50);
                ks2[i] = new Keyframe(t, 1f - Mathf.Cos(t * Mathf.PI * 0.5f));
            }
            easeOut.keys = ks2;

            // Quadratic
            Keyframe[] ks3 = new Keyframe[50];
            for (int i = 0; i < 50; i++)
            {
                float t = ((float)i / 50);
                ks3[i] = new Keyframe(t, t * t);
            }
            quadratic.keys = ks3;

            // Smooth Step
            Keyframe[] ks4 = new Keyframe[50];
            for (int i = 0; i < 50; i++)
            {
                float t = ((float)i / 50);
                ks4[i] = new Keyframe(t, t * t * (3f - 2f * t));
            }
            smoothStep.keys = ks4;

            // Smoother Step
            Keyframe[] ks5 = new Keyframe[50];
            for (int i = 0; i < 50; i++)
            {
                float t = ((float)i / 50);
                ks5[i] = new Keyframe(t, t * t * t * (t * (6f * t - 15f) + 10f));
            }
            smootherStep.keys = ks5;

            // Linear
            Keyframe[] ks6 = new Keyframe[50];
            for (int i = 0; i < 50; i++)
            {
                float t = ((float)i / 50);
                ks6[i] = new Keyframe(t, t);
            }
            linear.keys = ks6;

            firstRun = false;
        }
        CinematicCameraMovement cm = (CinematicCameraMovement)target;

        GUIStyle titleStyle = new GUIStyle();
        titleStyle.fontSize = 18;
        titleStyle.fontStyle = FontStyle.Bold;
        GUIStyle h1Style = new GUIStyle();
        h1Style.fontSize = 16;
        GUIStyle h2Style = new GUIStyle();
        h2Style.fontSize = 14;
        h2Style.fontStyle = FontStyle.Italic;
        GUILayout.Label("Movements", titleStyle);
        for (int i = 0; i < cm.movements.Count; i++)
        {
            GUILayout.BeginHorizontal();
            GUILayout.Label("Movement " + (i+1), h1Style);
            if(GUILayout.Button("Remove"))
            {
                cm.movements.RemoveAt(i);
                return;
            }
            GUILayout.EndHorizontal();

            GUILayout.BeginHorizontal();
            GUILayout.Label("Transition Type");
            cm.movements[i].transitionType = (TransitionType)EditorGUILayout.EnumPopup(cm.movements[i].transitionType);
            switch (cm.movements[i].transitionType)
            {
                case TransitionType.CustomCurve:
                    cm.movements[i].transitionCurve = EditorGUILayout.CurveField(cm.movements[i].transitionCurve);
                    break;
                case TransitionType.EaseOut:
                    EditorGUILayout.CurveField(easeOut);
                    break;
                case TransitionType.EaseIn:
                    EditorGUILayout.CurveField(easeIn);
                    break;
                case TransitionType.Quadratic:
                    EditorGUILayout.CurveField(quadratic);
                    break;
                case TransitionType.SmoothStep:
                    EditorGUILayout.CurveField(smoothStep);
                    break;
                case TransitionType.SmootherStep:
                    EditorGUILayout.CurveField(smootherStep);
                    break;
                case TransitionType.Linear:
                    EditorGUILayout.CurveField(linear);
                    break;
            }
            GUILayout.EndHorizontal();

            GUILayout.Label("Starting Point", h2Style);
            cm.movements[i].point1.position = EditorGUILayout.Vector3Field("Position", cm.movements[i].point1.position);
            cm.movements[i].point1.rotation = EditorGUILayout.Vector3Field("Rotation", cm.movements[i].point1.rotation);

            GUILayout.Label("Ending Point", h2Style);
            cm.movements[i].point2.position = EditorGUILayout.Vector3Field("Position", cm.movements[i].point2.position);
            cm.movements[i].point2.rotation = EditorGUILayout.Vector3Field("Rotation", cm.movements[i].point2.rotation);
        }
        if (GUILayout.Button("New Movement"))
        {
            Movement newMovement = new Movement();
            if (cm.movements.Count > 1)
            {
                newMovement.point1 = cm.movements[cm.movements.Count - 2].point2;
            }
            else
            {
                Waypoint wp1 = new Waypoint();
                wp1.position = new Vector3();
                wp1.rotation = new Vector3();
                newMovement.point1 = wp1;
            }
            Waypoint wp2 = new Waypoint();
            wp2.position = newMovement.point1.position;
            wp2.rotation = newMovement.point1.rotation;
            newMovement.point2 = wp2;

            cm.movements.Add(newMovement);
        }

        GUILayout.Label("To Do When Complete", titleStyle);
        this.serializedObject.Update();
        EditorGUILayout.PropertyField(this.serializedObject.FindProperty("whenMovementsComplete"), true);
        this.serializedObject.ApplyModifiedProperties();
    }
}