using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(LineRenderer))]
public class EllipseRenderer : MonoBehaviour
{
    public LineRenderer lineRenderer;

    [Range(3, 360)]
    public int segments;
    public Ellipse ellipse;

    private void Awake()
    {
        lineRenderer = GetComponent<LineRenderer>();
        CalculateEllipse();
    }

    void CalculateEllipse()
    {
        Vector3[] points = new Vector3[segments + 1];
        for (int i = 0; i < segments; i++)
        {
            Vector2 position2d = ellipse.Evaluate(((float)i / (float)segments));
            points[i] = new Vector3(position2d.x, position2d.y, 0f);

        }
        points[segments] = points[0];

        lineRenderer.positionCount = segments + 1;
        lineRenderer.SetPositions(points);
    }


    private void OnValidate()
    {
        CalculateEllipse();
    }
}