using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class SS_CanvasTrigger : MonoBehaviour
{
    private GameObject ss_gui;

    // Register the events Purchase
    private void Start() {
        ss_gui = GameObject.Find("SS_GUI");
        GameManager.instance.onPurchaseBegin += OnPurchaseBegin;
        GameManager.instance.onPurchaseAborted += OnPurchaseAborted;    
    }

    private void OnDestroy() {
        GameManager.instance.onPurchaseBegin -= OnPurchaseBegin;
        GameManager.instance.onPurchaseAborted -= OnPurchaseAborted;
    }

    private void OnPurchaseBegin() => ss_gui.SetActive(true);

    private void OnPurchaseAborted() => ss_gui.SetActive(false);
}