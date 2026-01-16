using InitialPrefabs.Attributes;
using UnityEditor;
using UnityEngine;

namespace InitialPrefabs.Editor.Attributes {

    [CustomPropertyDrawer(typeof(ProgressBarAttribute))]
    public class ProgressBarAttributeDrawer : BasePropertyDrawer {

        protected override void OnInspectorAttribute(Rect rect, SerializedProperty prop, GUIContent label) {
            var progressBar = attribute as ProgressBarAttribute;

            if (IsTypeNumeric(prop)) {
                var singleLineHeight = rect.height / 2;
                var propRect = new Rect(rect.x, rect.y, rect.width, singleLineHeight);

                ClampNumericValue(prop);
                EditorGUI.PropertyField(propRect, prop);

                var progressRect = new Rect(rect.x, rect.y + singleLineHeight, rect.width, rect.height -
                    singleLineHeight);

                DrawProgressBar(progressRect, prop);
            } else {
                EditorGUI.PropertyField(rect, prop);
            }
        }

        public override float GetPropertyHeight(SerializedProperty property, GUIContent label) {
            var progressBar = attribute as ProgressBarAttribute;
            var height = EditorGUI.GetPropertyHeight(property);

            if (IsTypeNumeric(property)) {
                return height * 2;
            }
            return height;
        }

        private bool IsTypeNumeric(SerializedProperty prop) =>
            prop.propertyType == SerializedPropertyType.Integer || prop.propertyType == SerializedPropertyType.Float;

        private void DrawProgressBar(Rect r, SerializedProperty prop) {
            var progressBar = attribute as ProgressBarAttribute;
            switch (prop.propertyType) {
                case SerializedPropertyType.Integer:
                    EditorGUI.ProgressBar(r, prop.intValue / progressBar.max, 
                            $"{progressBar.label}: {prop.intValue}/{progressBar.max}");
                    return;
                case SerializedPropertyType.Float:
                    EditorGUI.ProgressBar(r, prop.floatValue / progressBar.max, 
                            $"{progressBar.label}: {prop.floatValue}/{progressBar.max}");
                    return;
                default:
                    Debug.LogError($"{fieldInfo.Name} is not a numeric type!");
                    return;
            }
        }

        private void ClampNumericValue(SerializedProperty prop) {
            var progressBar = attribute as ProgressBarAttribute;
            switch (prop.propertyType) {
                case SerializedPropertyType.Integer:
                    prop.intValue = Mathf.Clamp(prop.intValue, 0, (int)progressBar.max);
                    return;
                case SerializedPropertyType.Float:
                    prop.floatValue = Mathf.Clamp(prop.floatValue, 0, progressBar.max);
                    return;
                default:
                    Debug.LogError($"{fieldInfo.Name} is not a numeric type!");
                    return;
            }
        }
    }
}