using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlantableArea : MonoBehaviour
{
    public Vector2 boundSize;
    private Bounds plantableBoundary;

    // Set the boundaries from the top-left
    void Start()
    {
        gameObject.tag = "PlantableArea";
        plantableBoundary = new Bounds(transform.position, boundSize);
    }

    /**
    Checks if the Bounds object contains a given point
    @param point The point that should be checked for inside the bounds
    @return True if the point was inside the bounds, false if not
    */
    public bool ContainsPoint(Vector3 point) {
        return plantableBoundary.Contains(point);
    }

    //Draw debug outlines
    void OnDrawGizmos() {
        Gizmos.color = Color.yellow;
        Gizmos.DrawWireCube(transform.position, boundSize);
    }

}