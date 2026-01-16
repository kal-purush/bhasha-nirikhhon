using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;
using UnityEngine.XR.Interaction.Toolkit;



public class lightController : MonoBehaviour
{
    bool active;
    public bool beingHeld;
    public InputActionReference lightActionReference;


    void Holdable(SelectEnterEventArgs args)
    {
        beingHeld = !beingHeld;
    }
    // Update is called once per frame
    private void Update()
    {
        lightActionReference.action.performed += LightSwitcher;
        if(active == true)
        {
            Debug.Log("Position to be sent to enemy:" + transform.position);
        }
    }

    void LightSwitcher(InputAction.CallbackContext obj)
    {

            active = !active;
            GetComponent<Light>().enabled = active;
            Debug.Log("button got pressed");
        
    }
}