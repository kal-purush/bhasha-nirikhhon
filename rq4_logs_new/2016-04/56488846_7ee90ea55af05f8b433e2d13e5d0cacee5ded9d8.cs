using UnityEngine;
using UnityEditor;
using UnityEditorInternal;
using System.IO;



[CanEditMultipleObjects]
[CustomEditor(typeof(RailManager))]
public class RailManagerEditor : Editor {

    public static Component GetSerializedPropertyRootComponent(SerializedProperty property)
    {
        return (Component)property.serializedObject.targetObject;
    }

    private ReorderableList nodeList;

    private void OnEnable()
    {
        nodeList = new ReorderableList(serializedObject,
                                   serializedObject.FindProperty("railNodes"),
                                   true, true, true, true);

        nodeList.drawElementCallback =
        (Rect rect, int index, bool isActive, bool isFocused) =>
        {
            var element = nodeList.serializedProperty.GetArrayElementAtIndex(index);
            rect.y += 2;

            GUIContent typeContent = new GUIContent("Node " + index + ":", "");
            Rect typeRect = new Rect(rect.x, rect.y, 50, EditorGUIUtility.singleLineHeight);
            EditorGUI.LabelField(typeRect, typeContent);

            Rect prefabRect = new Rect(rect.x + 55, rect.y, rect.width - 60, EditorGUIUtility.singleLineHeight);
            EditorGUI.PropertyField(prefabRect, element.FindPropertyRelative("transform"), GUIContent.none);
        };

        nodeList.drawHeaderCallback = (Rect rect) =>
        {
            EditorGUI.LabelField(rect, "Nodes");
        };

        nodeList.onSelectCallback = (ReorderableList l) =>
        {
            var transform_ = l.serializedProperty.GetArrayElementAtIndex(l.index).FindPropertyRelative("transform").objectReferenceValue as Transform;
            if (transform_)
                EditorGUIUtility.PingObject(transform_.gameObject);
        };

        nodeList.onRemoveCallback = (ReorderableList l) =>
        {
            ReorderableList.defaultBehaviours.DoRemoveButton(l);
        };

        nodeList.onAddCallback = (ReorderableList l) =>
        {
            
            Node node = new Node();
            GameObject obj = new GameObject("Node");
            obj.transform.parent = GameObject.FindObjectOfType<RailManager>().transform;                           
            node.transform = obj.transform;

            int index = nodeList.serializedProperty.arraySize;
            nodeList.serializedProperty.arraySize++;
            nodeList.index = index;
            SerializedProperty element = nodeList.serializedProperty.GetArrayElementAtIndex(index);
            element.FindPropertyRelative("transform").objectReferenceValue = node.transform;
            serializedObject.ApplyModifiedProperties();
            
            //var index = l.serializedProperty.arraySize;
            //l.serializedProperty.arraySize++;
            //l.index = index;
            //var element = l.serializedProperty.GetArrayElementAtIndex(index);
            //element.FindPropertyRelative("transform").objectReferenceValue = new GameObject("Node").transform;
        };

    }

    private void clickHandler(/*object target*/)
    {
        

    }

    public override void OnInspectorGUI()
    {
        DrawDefaultInspector();
        GUILayout.Space(10);
        serializedObject.Update();
        nodeList.DoLayoutList();
        serializedObject.ApplyModifiedProperties();
        GUILayout.Space(10);
    }

}