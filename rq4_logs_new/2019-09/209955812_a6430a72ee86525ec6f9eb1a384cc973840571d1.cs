using UnityEngine;
using UnityEditor;


namespace manager.ioc
{
    [CustomPropertyDrawer(typeof(ReadOnlyAttribute))]
    public class ReadOnlyDrawer : PropertyDrawer
    {
        

        public override float GetPropertyHeight(SerializedProperty _Property,
                                                GUIContent _Label)
        {
            return EditorGUI.GetPropertyHeight(_Property, _Label, true);
        }

        public override void OnGUI(Rect _Position,
                                   SerializedProperty _Property,
                                   GUIContent _Label)
        {
            ReadOnlyAttribute Attribute = (ReadOnlyAttribute)attribute;

            if (Attribute.ReadOnly)
            {
                GUI.enabled = false;
                EditorGUI.PropertyField(_Position, _Property, _Label, true);
                GUI.enabled = true;
            }
            else
            {
                EditorGUI.PropertyField(_Position, _Property, _Label, true);
            }
        }
    }
}