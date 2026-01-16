using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DoorLock : Interactable
{
    public Doors controlledDoor;

    #region Methods

    //Overriding methods
    public override void PlayerInteract(Player player)
    {
        base.PlayerInteract(player);
        InteractDoorLock();
    }

    public override Instruction EntityInteract(Entity entity)
    {
        InteractDoorLock();
        return new Repair(5.0f, this, entity);
    }

    void InteractDoorLock()
    {
        controlledDoor.SetLockedState();
    }

    #endregion

    #region Functions
    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.layer == 9)
        {
            Debug.Log(other.gameObject.name + " entered the door panel range");
            //Start showing the UI
        }
    }

    void OnTriggerStay(Collider other)
    {
        if (other.gameObject.layer == 9)
        {
            if (Input.GetButtonDown("Interact") && Time.time - lastInteractionTime > cooldownTime)
            {
                Debug.Log("Player interacted with the door panel");
                PlayerInteract(other.GetComponent<Player>());
                // Interact cooldown set:
                lastInteractionTime = Time.time;
            }
        }
        if (other.gameObject.layer == 10)
        {

        }
    }

    void OnTriggerExit(Collider other)
    {
        if (other.gameObject.layer == 9)
        {
            Debug.Log(other.gameObject.name + " left the door panel range");
            //Stop showing the UI
        }
    }
    #endregion
}