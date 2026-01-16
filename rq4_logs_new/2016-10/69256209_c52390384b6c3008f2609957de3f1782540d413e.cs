using UnityEngine;
using System.Collections;
using System;

public class PickUpObject : MonoBehaviour, IInteractableObject
{
    bool m_inUse = false;
    bool m_isUsable = true;
    bool m_highlightOnTouch = true;

    public bool inUse
    {
        get
        { return m_inUse; }

        set
        { m_inUse = value; }
    }
    public bool isUsable
    {
        get
        {   return m_isUsable; }

        set
        {   m_isUsable = value;    }
    }
    public bool highlightOnTouch
    {
        get
        {   return m_highlightOnTouch;  }

        set
        {   m_highlightOnTouch = value; }
    }

    [SerializeField] GameObject lefthandSpawnObject;
    [SerializeField] GameObject righthandSpawnObject;
    [SerializeField] GameObject highlightObjects;
    [SerializeField] GameObject model;
    SteamVR_ControllerManager controlManager;

    GameObject leftControllerModel;
    GameObject rightControllerModel;

    void Awake()
    {
        controlManager = GameObject.FindObjectOfType<SteamVR_ControllerManager>();
        leftControllerModel = controlManager.left.GetComponentInChildren<SteamVR_RenderModel>().gameObject;
        rightControllerModel = controlManager.right.GetComponentInChildren<SteamVR_RenderModel>().gameObject;
    }

    public void Touch(bool beingTouched)
    {
        if(highlightOnTouch && !inUse)
        {
            highlightObjects.SetActive(beingTouched);
        }
    }

    public void Interact()
    {
        if(inUse)   // Put Down
        {
            model.SetActive(true);
            inUse = false;
            foreach(Transform child in controlManager.left.transform)
            {
                if (child.gameObject.tag == "PickUp")
                {
                    Destroy(child.gameObject);
                }
            }
            foreach (Transform child in controlManager.right.transform)
            {
                if (child.gameObject.tag == "PickUp")
                {
                    Destroy(child.gameObject);
                }
            }
            leftControllerModel.SetActive(true);
            rightControllerModel.SetActive(true);
        }

        else    // Pick Up
        {
            if (isUsable)
            {
                model.SetActive(false);

                GameObject gl =  Instantiate(lefthandSpawnObject);
                GameObject gr =  Instantiate(righthandSpawnObject);
                gl.transform.parent = controlManager.left.transform;
                gl.transform.localPosition = lefthandSpawnObject.transform.position;
                gl.transform.localRotation = lefthandSpawnObject.transform.rotation;
                gr.transform.parent = controlManager.right.transform;
                gr.transform.localPosition = righthandSpawnObject.transform.position;
                gr.transform.localRotation = righthandSpawnObject.transform.rotation;

                inUse = true;

                leftControllerModel.SetActive(false);
                rightControllerModel.SetActive(false);
            }
            
            highlightObjects.SetActive(true);
            
        }
    }

    void PickUp()
    {

    }

    void PutBack()
    {

    }
}