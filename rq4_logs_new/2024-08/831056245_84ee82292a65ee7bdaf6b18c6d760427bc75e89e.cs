using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UltimateXR.Manipulation;
public class TurnTriggerOnPlaced : MonoBehaviour
{
    [SerializeField] UxrGrabbableObject uxrGrabbableObject;
    [SerializeField] Collider collider;

    private void Start() {
        uxrGrabbableObject = GetComponent<UxrGrabbableObject>();
    }
    private void OnEnable() {
        UxrGrabManager.Instance.ObjectPlaced += IsPlaced;
         UxrGrabManager.Instance.ObjectRemoved += IsRemoved;
    }

    private void OnDisable() {
        UxrGrabManager.Instance.ObjectPlaced -= IsPlaced;
        UxrGrabManager.Instance.ObjectRemoved -= IsRemoved;
    }

    private void IsPlaced(object sender, UxrManipulationEventArgs e) {
        if (e.GrabbableObject != uxrGrabbableObject) return;
        collider.isTrigger = true;
    }

    private void IsRemoved(object sender, UxrManipulationEventArgs e) {
        if (e.GrabbableObject != uxrGrabbableObject) return;
        collider.isTrigger = false;
    }
}