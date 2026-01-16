using UnityEngine;
using UnityEditor;

public class ReplaceMaterialEditor : EditorWindow
{
    private Material materialToReplace;
    private Material newMaterial;

    [MenuItem("Tools/Replace Material")]
    public static void ShowWindow()
    {
        GetWindow<ReplaceMaterialEditor>("Replace Material");
    }

    void OnGUI()
    {
        GUILayout.Label("Replace Material", EditorStyles.boldLabel);

        materialToReplace = (Material)EditorGUILayout.ObjectField("Material to Replace", materialToReplace, typeof(Material), false);
        newMaterial = (Material)EditorGUILayout.ObjectField("New Material", newMaterial, typeof(Material), false);

        if (GUILayout.Button("Replace"))
        {
            ReplaceMaterials();
        }
    }

    void ReplaceMaterials()
    {
        if (materialToReplace == null || newMaterial == null)
        {
            Debug.LogWarning("Please assign both materials.");
            return;
        }

        // Get all MeshRenderer components in the scene
        MeshRenderer[] renderers = FindObjectsOfType<MeshRenderer>();

        foreach (MeshRenderer renderer in renderers)
        {
            // Loop through all materials in the renderer
            for (int i = 0; i < renderer.sharedMaterials.Length; i++)
            {
                // Check if the current material is the one we want to replace
                if (renderer.sharedMaterials[i] == materialToReplace)
                {
                    // Create a new array to hold the materials
                    Material[] materials = renderer.sharedMaterials;

                    // Replace the material
                    materials[i] = newMaterial;

                    // Assign the new array back to the renderer
                    renderer.sharedMaterials = materials;

                    // Mark the object as dirty so the change is saved
                    EditorUtility.SetDirty(renderer);
                }
            }
        }

        Debug.Log("Material replacement complete.");
    }
}