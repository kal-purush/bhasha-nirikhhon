using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[ExecuteInEditMode]
public class LayeredLiquidController : MonoBehaviour
{
    public GameObject bottomLiquid;
    public GameObject topLiquid;
    public GameObject alignmentPoint;

    [Range(0.0f, 0.05f)]
    public float firstLiquidScale = 0.0f;

    [Range(0.0f, 0.05f)]
    public float secondLiquidScale = 0.0f;

    [Range(0.0f, 0.1f)]
    public float maxTotalScale = 0.03f;

    void Start()
    {
        if (!Application.isPlaying)
            return;

        if (bottomLiquid == null || topLiquid == null || alignmentPoint == null)
        {
            Debug.LogError("bottomLiquid, topLiquid, or alignmentPoint is not assigned.");
            return;
        }

        AlignAndScaleObjects();
    }

    private void Update()
    {
        AlignAndScaleObjects();
    }

    void OnValidate()
    {
        AlignAndScaleObjects();
    }

    void AlignAndScaleObjects()
    {
        if (bottomLiquid == null || topLiquid == null || alignmentPoint == null)
            return;

        // Ensure the total scale does not exceed the maxTotalScale
        float totalScale = firstLiquidScale + secondLiquidScale;

        if (totalScale > maxTotalScale)
        {
            float scaleFactor = maxTotalScale / totalScale;
            firstLiquidScale *= scaleFactor;
            secondLiquidScale *= scaleFactor;
        }

        // Scale bottomLiquid
        Vector3 currentOne = bottomLiquid.transform.localScale;
        currentOne.y = firstLiquidScale;
        bottomLiquid.transform.localScale = currentOne;

        MeshRenderer renderer1 = bottomLiquid.GetComponent<MeshRenderer>();
        if (renderer1 != null)
        {
            renderer1.enabled = firstLiquidScale > 0.0f;
        }

        // Scale topLiquid
        Vector3 currentTwo = topLiquid.transform.localScale;
        currentTwo.y = secondLiquidScale;
        topLiquid.transform.localScale = currentTwo;

        MeshRenderer renderer2 = topLiquid.GetComponent<MeshRenderer>();
        if (renderer2 != null)
        {
            renderer2.enabled = secondLiquidScale > 0.0f;
        }

        // Align and position liquids
        AlignObjectsOnTop();
    }

    void AlignObjectsOnTop()
    {
        Renderer renderer1 = bottomLiquid.GetComponent<Renderer>();
        Renderer renderer2 = topLiquid.GetComponent<Renderer>();

        if (renderer1 == null || renderer2 == null)
        {
            Debug.LogError("Renderer component is missing on one of the objects.");
            return;
        }

        Bounds bounds1 = renderer1.bounds;
        Bounds bounds2 = renderer2.bounds;

        // Calculate the offset between the two objects
        Vector3 offset = new Vector3(0, bounds1.extents.y + bounds2.extents.y, 0);

        // Calculate the position of the bottomLiquid based on the alignment point
        Vector3 alignmentPointPosition = alignmentPoint.transform.position;

        // Calculate the local position of bottomLiquid in the parent space
        Vector3 localPositionBottomLiquid = new Vector3(0, bounds1.extents.y, 0);
        
        // Set the local position of bottomLiquid in parent space
        bottomLiquid.transform.localPosition = localPositionBottomLiquid;

        // Calculate the position of topLiquid based on the bottomLiquid position
        Vector3 localPositionTopLiquid = localPositionBottomLiquid + offset;

        // Set the local position of topLiquid in parent space
        topLiquid.transform.localPosition = localPositionTopLiquid;

        // If necessary, you may want to correct for parent's local position and rotation
        // Adjust the position of bottomLiquid and topLiquid relative to the parent object's transform
        bottomLiquid.transform.position = transform.TransformPoint(bottomLiquid.transform.localPosition);
        topLiquid.transform.position = transform.TransformPoint(topLiquid.transform.localPosition);
    }
}