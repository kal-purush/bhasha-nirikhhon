using UGF.EditorTools.Editor.IMGUI;
using UGF.EditorTools.Editor.IMGUI.Scopes;
using UGF.Models.Runtime.Collections;
using UnityEditor;

namespace UGF.Models.Editor.Collections
{
    [CustomEditor(typeof(CollectionDictionaryModelAsset), true)]
    internal class CollectionDictionaryModelAssetEditor : UnityEditor.Editor
    {
        private ReorderableListKeyAndValueDrawer m_listItems;

        private void OnEnable()
        {
            m_listItems = new ReorderableListKeyAndValueDrawer(serializedObject.FindProperty("m_items"));
            m_listItems.Enable();
        }

        private void OnDisable()
        {
            m_listItems.Disable();
        }

        public override void OnInspectorGUI()
        {
            using (new SerializedObjectUpdateScope(serializedObject))
            {
                EditorIMGUIUtility.DrawScriptProperty(serializedObject);

                m_listItems.DrawGUILayout();
            }
        }
    }
}