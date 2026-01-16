using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ViewManager : MonoBehaviour
{
    public CreateViewManager createViewManager;
    public GameObject crowdView;

    private Transform crowdViewTransform;

    void Awake() {
        crowdViewTransform = crowdView.GetComponent<Transform>();
    }

    public void setToCrowdView() {
        createViewManager.getCurrentCreatedCharacter().GetComponent<Transform>().SetParent(crowdViewTransform);
    }
}