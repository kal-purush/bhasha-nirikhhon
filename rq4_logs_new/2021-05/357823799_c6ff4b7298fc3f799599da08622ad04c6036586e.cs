using UnityEditor;
using UnityEditor.UI;
using UnityEditor.UIElements;
using UnityEngine.UIElements;
using Game.Tweening;

[CustomEditor(typeof(CustomButton))]
public class CustomButtonEditor : ButtonEditor
{
    private SerializedProperty m_InteractableProperty;

    protected override void OnEnable()
    {
        m_InteractableProperty  = serializedObject.FindProperty("m_Interactable");
    }
  
    public override VisualElement CreateInspectorGUI()
    {
        var root = new VisualElement();
      
        var changeButtonType  = new PropertyField(serializedObject.FindProperty(CustomButton.ChangeButtonType));
        var curveEase  = new PropertyField(serializedObject.FindProperty(CustomButton.CurveEase));
        var duration  = new PropertyField(serializedObject.FindProperty(CustomButton.Duration));
        var tweenLabel = new Label("Settings Tween");
        var intractableLabel = new Label("Intractable");

        root.Add(tweenLabel);
        root.Add(changeButtonType);
        root.Add(curveEase);
        root.Add(duration);
      
        root.Add(intractableLabel);
        root.Add(new IMGUIContainer(OnInspectorGUI));

        return root;
    }

    public override void OnInspectorGUI()
    {
        serializedObject.Update();

        EditorGUILayout.PropertyField(m_InteractableProperty);
          
        EditorGUI.BeginChangeCheck();

        serializedObject.ApplyModifiedProperties();
    }
}