using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class OrderKioskBehavior : MonoBehaviour
{
    public Text hintText;
    public Transform player;

    private bool isOrdering;
    private bool isLookingAtKiosk;
    private bool inKioskTransition;
    private Vector3 originalPlayerPosition;
    private Vector3 wantedPlayerLocation;
    
    // Start is called before the first frame update
    void Start()
    {
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(Controls.ENTER_ORDER) && !isOrdering && isLookingAtKiosk)
        {
            Debug.Log("ENTERING ORDER");
            // Lock player movement and look explicitly
            PlayerMovementController.locked = true;
            PlayerLookController.locked = true;
            PlayerLookController.HideWeapons();
            isOrdering = true;
            
            // Move player to kiosk smoothly
            originalPlayerPosition = new Vector3(player.position.x, player.position.y, player.position.z);
            inKioskTransition = true;
            wantedPlayerLocation = transform.position + transform.TransformDirection( new Vector3(0, -0.65f, 1.25f) );
        }
        
        if (Input.GetKeyDown(Controls.EXIT_ORDER) && isOrdering)
        {
            Debug.Log("EXITING ORDER");
            isOrdering = false;
            PlayerLookController.ShowWeapons();

            // Move player back to original position smoothly
            inKioskTransition = true;
            wantedPlayerLocation = originalPlayerPosition;
        }
        
        // Smoothly move player to kiosk or back to original position if in transition state
        if (inKioskTransition)
        {
            // Move player to kiosk smoothly
            player.position = Vector3.Lerp(player.position, wantedPlayerLocation, Time.deltaTime * 5f);
            // Make player rotate to face kiosk smoothly
            // Instead of transform.rotation, it's 180 degrees from transform.rotation to make player face the kiosk
            Camera.main.transform.rotation = Quaternion.Lerp(Camera.main.transform.rotation, transform.rotation * Quaternion.Euler(0, 180, 0), Time.deltaTime * 5f);
            
            if (Vector3.Distance(player.position, wantedPlayerLocation) < 0.01f)
            {
                inKioskTransition = false;
                OnKioskTransitionComplete();
            }
        }
    }

    private void OnKioskTransitionComplete()
    {
        PlayerLookController.locked = isOrdering;
        PlayerMovementController.locked = isOrdering;

        Debug.Log("TRANSITION COMPLETE");
    }

    private void OnMouseOver()
    {
        float dist = Vector3.Distance(player.position, transform.position);
        if (dist < 5 && !isOrdering)
        {
            hintText.gameObject.SetActive(true);
            isLookingAtKiosk = true;
        }
        else
        {
            hintText.gameObject.SetActive(false);
            isLookingAtKiosk = false;
        }
    }
    
    private void OnMouseExit()
    {
        hintText.gameObject.SetActive(false);
        isLookingAtKiosk = false;
    }
}