using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ButtonActivatedGate : MonoBehaviour
{
    public float activationSpeed = 5;

    private float originalScaleY;
    private float yScale;
    private bool isOpen;

    void Start()
    {
        originalScaleY = transform.localScale.y;
        yScale = originalScaleY;
        isOpen = false;
    }

    void Update()
    {
        transform.localScale = new Vector3(transform.localScale.x, Mathf.Lerp(transform.localScale.y, yScale, activationSpeed * Time.deltaTime), transform.localScale.z);
    }

    public void Activate()
    {
        yScale = 0;
        if (isOpen) //gate is currently open so we need to close it on re-activation
        {
            yScale = originalScaleY;
        }

        isOpen = !isOpen;
    }
}