using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.U2D;

public class SoftBody : MonoBehaviour
{
    private const float splineOffset = 0.5f;

    [SerializeField] public SpriteShapeController spriteShape;
    [SerializeField] public Transform[] jigglePoints;

    private void Awake()
    {
        UpdateVerticies();
    }

    private void Update()
    {
        UpdateVerticies();
    }

    private void UpdateVerticies()
    {
        for (int i = 0; i < jigglePoints.Length - 1; i++) 
        {
            Vector2 vertexVar = jigglePoints[i].localPosition;
            Vector2 towardsCenter = (Vector2.zero - vertexVar).normalized;
            float colliderRadius = jigglePoints[i].gameObject.GetComponent<CircleCollider2D>().radius;

            try
            {
                spriteShape.spline.SetPosition(i, (vertexVar - towardsCenter * colliderRadius));
            }
            catch
            {
                Debug.Log("Spline points are too close to each other.. recalculate");
                spriteShape.spline.SetPosition(i, (vertexVar - towardsCenter * (colliderRadius + splineOffset)));
            }

            Vector2 leftTangent = spriteShape.spline.GetLeftTangent(i);

            Vector2 newRightTangent = Vector2.Perpendicular(towardsCenter) * leftTangent.magnitude;
            Vector2 newLeftTangent = Vector2.zero - (newRightTangent);

            spriteShape.spline.SetRightTangent(i, newRightTangent);
            spriteShape.spline.SetLeftTangent(i, newLeftTangent);
        }
    }

}