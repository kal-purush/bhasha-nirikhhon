using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GoToPointAction : Action
{
    public Transform targetEntity = null;
    public Transform targetPoint;
    public float speedMetersPerSecond = 1.4f; // To be changed to an enum, walking, running...

    private Vector3 sourcePosition;
    private Vector3 path;
    private float t;

    protected override bool StartDerived()
    {
        sourcePosition = targetEntity.position;
        path = targetPoint.position - targetEntity.position;
        t = 0;
        targetEntity.LookAt(targetPoint); // To be changed by a smooth rotation started here and performed in 'UpdateDerived'

        return false;
    }

    protected override bool UpdateDerived()
    {
        // To be changed by nav-mesh code
        t += Time.deltaTime * speedMetersPerSecond;
        t = Mathf.Min(t, 1.0f);
        targetEntity.position = sourcePosition + path * t;

        return t == 1.0f;
    }

    protected override Action CloneDerived()
    {
        GoToPointAction clone = new GoToPointAction();
        clone.targetEntity = null;
        clone.targetPoint = this.targetPoint;
        clone.speedMetersPerSecond = this.speedMetersPerSecond;

        return clone;
    }
}