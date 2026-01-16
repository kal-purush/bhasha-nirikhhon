using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CameraSensitive_Y : MonoBehaviour
{
    public Slider sensitiveSlider;
    private SaveManager_Y saveManager;
    private TpsCameraJC_R cameraScr;

    // Start is called before the first frame update
    void Start()
    {
        var saveObj = GameObject.FindGameObjectWithTag("SaveManager");
        if (saveObj == null)
        {
            saveManager = saveObj.GetComponent<SaveManager_Y>();
            sensitiveSlider.value = saveManager.GetCameraSensitive();
        }
        else
        {
            cameraScr = GameObject.FindGameObjectWithTag("MainCamera").GetComponent<TpsCameraJC_R>();
        }
    }

    // Update is called once per frame
    void Update()
    {
        if (saveManager != null)
        {
            saveManager.SaveCameraSensitive(sensitiveSlider.value);
        }
        else if (cameraScr != null)
        {
            cameraScr.SetSpinSpeed(sensitiveSlider.value);
        }
    }
}