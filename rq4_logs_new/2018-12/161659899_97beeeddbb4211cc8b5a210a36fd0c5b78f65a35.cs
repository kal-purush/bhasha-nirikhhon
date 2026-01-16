using UnityEditor;
using UnityEngine;

namespace InitialPrefabs.Editor.Attributes {

    /// <summary>
    /// A base property drawer.
    /// </summary>
    public abstract class BasePropertyDrawer : PropertyDrawer {

        public override void OnGUI(Rect rect, SerializedProperty property, GUIContent label) {
            using (var scope = new EditorGUI.PropertyScope(rect, label, property)) {
                OnInspectorAttribute(rect, property, label);
            }
        }
    
        /// <summary>
        /// A utility function which is called during a PropertyScope. When the scope leaves the instance is 
        /// disposed.
        /// </summary>
        protected abstract void OnInspectorAttribute(Rect rect, SerializedProperty prop, GUIContent label);
    }
}