using UnityEngine;
using UnityEditor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

public static class EditorGUIHelper
{
    public static void guiColorForState(Transform targ, bool state, Color trueColor, Color falseColor, bool wantIcon = true, int trueIconIndex = 3, int falseIconIndex = 2) {
        GUIStyle style = EditorStyles.helpBox;
        GUI.backgroundColor = state ? Color.green : new Color(.3f, .7f, 1f);
        EditorGUILayout.BeginVertical(style);
        EditorGUILayout.LabelField(state ? "TRUE" : "FALSE");
        EditorGUILayout.EndVertical();

        //ICON
        DrawIcon(targ.gameObject, state ? 3 : 2);
    }

    // courtesy of: http://answers.unity3d.com/questions/213140/programmatically-assign-an-editor-icon-to-a-game-o.html
    public static void DrawIcon(GameObject gameObject, int idx) {
        var largeIcons = GetTextures("sv_label_", string.Empty, 0, 8);
        var icon = largeIcons[idx];
        var egu = typeof(EditorGUIUtility);
        var flags = BindingFlags.InvokeMethod | BindingFlags.Static | BindingFlags.NonPublic;
        var args = new object[] { gameObject, icon.image };
        var setIcon = egu.GetMethod("SetIconForObject", flags, null, new Type[]{typeof(UnityEngine.Object), typeof(Texture2D)}, null);
        setIcon.Invoke(null, args);
    }

    public static GUIContent[] GetTextures(string baseName, string postFix, int startIndex, int count) {
        GUIContent[] array = new GUIContent[count];
        for (int i = 0; i < count; i++) {
            array[i] = EditorGUIUtility.IconContent(baseName + (startIndex + i) + postFix);
        }
        return array;
    }
}