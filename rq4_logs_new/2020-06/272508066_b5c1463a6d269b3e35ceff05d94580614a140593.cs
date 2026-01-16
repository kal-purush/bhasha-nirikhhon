using System.Collections;
using System.Collections.Generic;
using Assets.Scripts;
using Assets.Scripts.Constructions.ConstructionCrane;
using UnityEngine;

public class ConstructionMenu : MonoBehaviour
{
    public RectTransform StructurePanel;
    public int ComfortFramesBlock = 30;
    public Camera WorldCamera;

    private Vector2 _mousePosition;
    private FrameLocker _fl = new FrameLocker();
    private ConstructionCraneModel _ccm = new ConstructionCraneModel();

    // Start is called before the first frame update
    void Start()
    {
        if (StructurePanel)
        {
            StructurePanel.gameObject.SetActive(false);
        }

        _fl.LockFrames = ComfortFramesBlock;
        _ccm.WorldCamera = WorldCamera;
    }

    // Update is called once per frame
    void Update()
    {
//        if (Input.GetKey(KeyCode.Mouse0) && StructurePanel.gameObject.activeSelf && _fl.CheckFrame())
//        {
//            StructurePanel.gameObject.SetActive(false);
//            _fl.StartLock();
//        }

//        else 
        if (Input.GetKey(KeyCode.Mouse1) && StructurePanel && !StructurePanel.gameObject.activeSelf && _fl.CheckFrame()) 
        {
            if (_ccm.CastAnyRayFromScreen(Input.mousePosition, out RaycastHit hit))
            {
                if (hit.transform.gameObject.layer == LayerMask.NameToLayer("BuildingZone"))
                {
                    StructurePanel.position = Input.mousePosition;

                    StructurePanel.gameObject.SetActive(true);
                    _fl.StartLock();
                }
            }
        }

        if (Input.GetKey(KeyCode.Mouse1) && _fl.CheckFrame())
        {
            StructurePanel.gameObject.SetActive(false);
            _fl.StartLock();
        }

        _fl.RefineFrame();
    }

    public void ClickTheButton(int buttonNumber)
    {
        

        
    }
}