using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;

public class HoverDetector : MonoBehaviour
{
    private bool isHovering;
    public bool IsHovering { get => isHovering; set => isHovering = value; }

    #region Singleton

    public static HoverDetector instance;


    private void Awake()
    {
        if (instance != null)
        {
            Destroy(gameObject);
        }
        else
        {
            instance = this;
        }
    }

    #endregion Singleton

    // Update is called once per frame
    void Update()
    {
        IsHovering = EventSystem.current.IsPointerOverGameObject();
    }

    private void OnDisable()
    {
        IsHovering = false;
    }
}