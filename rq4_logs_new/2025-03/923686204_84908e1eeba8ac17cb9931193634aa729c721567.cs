using System;
using UnityEngine;

public class PreviewSystem : MonoBehaviour
{
    [SerializeField] private float previewYOffset = 0.06f;

    [SerializeField] private GameObject cellIndicator;
    [SerializeField] private GameObject previewObject;

    [SerializeField] private Material previewMaterialsPrefab;
    [SerializeField] private Material previewMaterialsInstance;

    private void Start()
    {
        previewMaterialsInstance = new Material(previewMaterialsPrefab);
        cellIndicator.SetActive(false);
    }

    public void StartShowingPlacementPreview(GameObject prefab, Vector2Int size)
    {
        previewObject = Instantiate(prefab);
        PreparePreview(previewObject);
        PrepareCursor(size);
        cellIndicator.SetActive(true);
    }

    private void PrepareCursor(Vector2Int size)
    {
        if (size.x > 0 || size.y > 0)
        {
            cellIndicator.transform.localScale = new Vector3(size.x, 1, size.y);
            cellIndicator.GetComponentInChildren<Renderer>().material.mainTextureScale = size;
        }
    }

    private void PreparePreview(GameObject previewObject)
    {
        Renderer[] renderers = previewObject.GetComponentsInChildren<Renderer>();
        foreach (Renderer renderer in renderers)
        {
            Material[] materials = renderer.materials;
            for (int x = 0; x < materials.Length; x++)
            {
                materials[x] = previewMaterialsInstance;
            }
            renderer.materials = materials;
        }
    }

    public void StopShowingPreview()
    {

    }
}