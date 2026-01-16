using UGF.EditorTools.Editor.IMGUI;
using UGF.EditorTools.Editor.IMGUI.Scopes;
using UGF.Models.Runtime.Domain;
using UnityEditor;

namespace UGF.Models.Editor.Domain
{
    [CustomEditor(typeof(DomainModelCollectionModelAsset), true)]
    public class DomainModelCollectionModelAssetEditor : UnityEditor.Editor
    {
        private ReorderableListDrawer m_listModels;

        private void OnEnable()
        {
            m_listModels = new ReorderableListDrawer(serializedObject.FindProperty("m_models"))
            {
                DisplayAsSingleLine = true
            };

            m_listModels.Enable();
        }

        private void OnDisable()
        {
            m_listModels.Disable();
        }

        public override void OnInspectorGUI()
        {
            using (new SerializedObjectUpdateScope(serializedObject))
            {
                EditorIMGUIUtility.DrawScriptProperty(serializedObject);

                m_listModels.DrawGUILayout();
            }
        }
    }
}