using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor;

[CustomEditor(typeof(ConeOfVision))]
public class VisionEditor : Editor
{
    void OnSceneGUI()
    {
        ConeOfVision cov = (ConeOfVision)target;
        Handles.color = Color.white;
        Handles.DrawWireArc(cov.transform.position, Vector3.up, Vector3.forward, 360, cov.visionRadius);

        Vector3 angleA = cov.DirectionFromAngle(-cov.visionAngle / 2, false);
        Vector3 angleB = cov.DirectionFromAngle(cov.visionAngle / 2, false);

        Handles.DrawLine(cov.transform.position, cov.transform.position + angleA * cov.visionRadius);
        Handles.DrawLine(cov.transform.position, cov.transform.position + angleB * cov.visionRadius);

        Handles.color = Color.red;
        foreach (Transform target in cov.listOfTargets)
        {
            Handles.DrawLine(cov.transform.position, target.position);
        }
    }
}