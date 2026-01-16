using UnityEngine;
using UnityEditor;
using System.Collections;
using System.Runtime.InteropServices;
using TreeEditor;

/// <summary>
/// Author: Matt Gipson
/// Contact: Deadwynn@gmail.com
/// Domain: www.livingvalkyrie.com
/// 
/// Description: MonsterEditor 
/// </summary>
[CustomEditor(typeof (Monster))]
public class MonsterEditor : Editor {
    #region Fields

    Monster monsterScript;
    bool visibleRange = false;
    bool transitionRange = false;
    bool spawnTypes;

    #endregion

    void Awake() {
        monsterScript = (Monster) target;
    }

    public override void OnInspectorGUI() {

        visibleRange = EditorGUILayout.Foldout(visibleRange, "Visibility Settings");
        if (visibleRange) {
            EditorGUI.indentLevel++;
            monsterScript.VariableVisibleTime = EditorGUILayout.Toggle("Variable visible time", monsterScript.VariableVisibleTime);

            if (monsterScript.VariableVisibleTime) {
                EditorGUILayout.LabelField("Maximum - Minimum Time");
                EditorGUILayout.BeginHorizontal();

                //Since I manually created the label above, I am using an overload of the Float Field so no default label 
                //is displayed immediately before it.
                monsterScript.VisibleTimeMax = EditorGUILayout.FloatField(monsterScript.VisibleTimeMax);
                monsterScript.VisibleTimeMin = EditorGUILayout.FloatField(monsterScript.VisibleTimeMin);
                EditorGUILayout.EndHorizontal();
            } else {
                EditorGUILayout.BeginHorizontal();
                EditorGUILayout.PrefixLabel("Visible Time:");

                //Again, I manually created a prefix label to display so I am overloading the float field so no
                //default label is shown
                monsterScript.VisibleTimeMax = EditorGUILayout.FloatField(monsterScript.VisibleTimeMax);
                EditorGUILayout.EndHorizontal();
            }

            EditorGUI.indentLevel--;
        }

        EditorGUILayout.Space();

        transitionRange = EditorGUILayout.Foldout(transitionRange, "Transition Settings");
        if (transitionRange) {
            EditorGUI.indentLevel++;

            monsterScript.VariableTransitionTime =
                EditorGUILayout.Toggle("Variable Transition time?", monsterScript.VariableTransitionTime);

            if (monsterScript.VariableTransitionTime) {
                EditorGUILayout.LabelField("Maximum - minmum time");
                EditorGUILayout.BeginHorizontal();

                monsterScript.TransitionTimeMax = EditorGUILayout.FloatField(monsterScript.TransitionTimeMax);
                monsterScript.TransitionTimeMin = EditorGUILayout.FloatField(monsterScript.TransitionTimeMin);

                EditorGUILayout.EndHorizontal();
            } else {
                EditorGUILayout.BeginHorizontal();
                EditorGUILayout.PrefixLabel("Transition time");
                monsterScript.TransitionTimeMax = EditorGUILayout.FloatField(monsterScript.TransitionTimeMax);

                EditorGUILayout.EndHorizontal();
            }

            EditorGUI.indentLevel--;
        }

        EditorGUILayout.Space();

        spawnTypes = EditorGUILayout.Foldout(spawnTypes, "Spawn Settings");
        if (spawnTypes) {
            EditorGUI.indentLevel++;
            serializedObject.Update();

            EditorGUILayout.PropertyField(serializedObject.FindProperty("possibleTypes"), true);
            EditorGUI.indentLevel--;
        }
        EditorGUILayout.Space();

        monsterScript.PointWorth = EditorGUILayout.IntField("Point Worth: ", monsterScript.PointWorth);

        serializedObject.ApplyModifiedProperties();
    }

}