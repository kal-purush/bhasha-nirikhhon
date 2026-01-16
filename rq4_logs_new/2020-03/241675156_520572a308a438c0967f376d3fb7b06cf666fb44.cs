using UnityEngine;
using UnityEditor;

public abstract class EditorWithSubEditors<TEditor, TTarget> : Editor
    where TEditor : Editor
    where TTarget : Object
{
    protected TEditor[] subEditors;


    public void CheckAndCreateSubEditors(TTarget[] subEditorTargets)
    {
        if (subEditors != null && subEditors.Length == subEditorTargets.Length)
        {
            return;
        }

        CleanupEditors();

        subEditors = new TEditor[subEditorTargets.Length];

        for (int i = 0; i < subEditors.Length; i++)
        {
            subEditors[i] = CreateEditor(subEditorTargets[i] as TTarget) as TEditor;
            SubEditorSetup(subEditors[i]);
        }
    }

    public void CleanupEditors()
    {
        if (subEditors == null)
            return;

        for (int i = 0; i < subEditors.Length; i++)
        {
            DestroyImmediate(subEditors[i]);
        }

        subEditors = null;
    }


    public abstract void SubEditorSetup(TEditor editor);
}