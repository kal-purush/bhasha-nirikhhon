using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class IKSolver : MonoBehaviour
{
    public Transform target;

    List<Axis> axes = new List<Axis>();

    public Transform joint1, joint2;

    Vector2 targetLocalPos;

    private void Start() {
        UpdateAxes(transform.GetChild(0));

    }


    private void Update() {
        UpdateTargetLocalPos();
        
        Debug.Log(targetLocalPos);
    }

    private void UpdateTargetLocalPos() {
        targetLocalPos = joint1.InverseTransformVector(target.position - joint1.position);
        targetLocalPos = new Vector2(-targetLocalPos.x, targetLocalPos.y);
    }   

    private void UpdateAxes(Transform joint) {
        axes.Add(joint.GetComponent<Axis>());

        if(joint.childCount > 0) {
            UpdateAxes(joint.GetChild(0));
        }
    }
}