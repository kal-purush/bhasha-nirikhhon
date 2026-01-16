using UnityEngine;
using UnityEditor;
using System.Collections;

namespace Staple.EditorScripts
{
    public class CustomProjectSettingsWindow : EditorWindow
    {
        [MenuItem("Utils/Project Settings")]
        public static void Init()
        {
            EditorWindow.GetWindow<CustomProjectSettingsWindow>("Project Settings", true);
        }

        private Editor spriteImportEditor;

        void Awake()
        {
            if (SpriteSettingsUtility.Prefs == null)
                SpriteSettingsUtility.LoadPrefs();
        }

        void OnGUI()
        {
            if (SpriteSettingsUtility.Prefs == null) return;

            spriteImportEditor = Editor.CreateEditor(SpriteSettingsUtility.Prefs);
            spriteImportEditor.OnInspectorGUI();
        }
    }
}