using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using FMODUnity;

public class ScannerFXBehaviours : MonoBehaviour
{
    HandScannerTouchPad touchPad;
    public MeshRenderer rendiBoi;
    [Space(10)]
    public StudioEventEmitter scanEmitter;
    public StudioEventEmitter finishEmitter;
    [Space(10)]
    public Material scanMaterial;
    public Material defaultMaterial;
    public Material completeMaterial;

    // Start is called before the first frame update
    void Start()
    {
        touchPad = GetComponent<HandScannerTouchPad>();
        if (defaultMaterial != null) rendiBoi.material = defaultMaterial;
    }

    public void ScanStartedMat()
    {
        if (scanMaterial != null) rendiBoi.material = scanMaterial;
    }

    public void ScanCancelledMat()
    {
        if (!touchPad.scanComplete)
        {
            if (defaultMaterial != null) rendiBoi.material = defaultMaterial;
        }
        
    }

    public void ScanFinishedMat()
    {
        if (completeMaterial != null) rendiBoi.material = completeMaterial;
        if (scanEmitter != null) scanEmitter.enabled = false;
        if (finishEmitter != null) finishEmitter.enabled = true;
    }

    public void ResetFromFinished()
    {
        if (defaultMaterial != null) rendiBoi.material = defaultMaterial;
        if (scanEmitter != null) scanEmitter.enabled = true;
        if (finishEmitter != null) finishEmitter.enabled = false;
    }
}