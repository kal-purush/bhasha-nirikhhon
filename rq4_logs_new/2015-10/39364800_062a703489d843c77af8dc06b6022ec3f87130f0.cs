using UnityEngine;
using System.Collections;

[ExecuteInEditMode]
[RequireComponent (typeof(EdgeCollider2D))]
public class CircleEdgeCollider2D : MonoBehaviour
{
  float CurrentInner = 0.0f;
  float CurrentOuter = 0.0f;
  EdgeCollider2D EdgeCollider;
  public float InnerRadius;
  public float OuterRadius;
  public int NumPoints;

  void Start()
  {
    CreateCircle();
  }

  void Update()
  {
    if(NumPoints != EdgeCollider.pointCount || 
       CurrentInner != InnerRadius ||
       CurrentOuter != OuterRadius)
    {
      CreateCircle();
    }
  }

  void CreateCircle()
  {
    Vector2[] edgePoints = new Vector2[NumPoints + 2];
    EdgeCollider = GetComponent<EdgeCollider2D>();

    for(int loop = 0; loop <= NumPoints / 2; loop++)
    {
      float angle = (Mathf.PI * 2.0f / NumPoints) * loop * 2;
      edgePoints[loop] = new Vector2(Mathf.Sin(angle), Mathf.Cos(angle)) * InnerRadius;
    }

    //edgePoints[NumPoints / 2 + 1] = new Vector2(0, 1)* (OuterRadius);
    for(int loop = NumPoints / 2 + 1; loop <= NumPoints + 1; loop++)
    {
      float angle = (Mathf.PI * 2.0f / NumPoints) * loop * 2;
      edgePoints[loop] = new Vector2(-Mathf.Sin(angle), Mathf.Cos(angle)) * (OuterRadius);
    }
    //edgePoints[0] = edgePoints[199];
    //edgePoints[200] = edgePoints[199];
    //edgePoints[201] = edgePoints[200];
    //edgePoints[75].Set(OuterRadius, 0);


    EdgeCollider.points = edgePoints;
    CurrentInner = InnerRadius;
    CurrentOuter = OuterRadius;
  }
}
