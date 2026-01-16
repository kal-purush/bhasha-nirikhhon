using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Door : Interactable
{
    public Sprite openSprite;
    public Sprite closedSprite;
    public bool isOpen = false;

    void Start() {
        SetDoorOpen(isOpen);
    }

    /*
    Sets whether or not the door is open. This will also update the door's sprite
    and collision to match the new open/closed state
    @param open True to open the door, false to close the door
    */
    void SetDoorOpen(bool open) {
        isOpen = open;

        //Update sprite
        SpriteRenderer doorRenderer = GetComponent<SpriteRenderer>();
        doorRenderer.sprite = isOpen ? openSprite : closedSprite;

        //Change the object's layer. This has the effect of updating collisions, as only collider objects on layer 8 collide with the player
        //This allows us to disable collisions without disabling the interactable
        gameObject.layer = isOpen ? 1 : 8;
    }

    //Interact function - open/close the door by toggling
    public override void Interact(Player triggerPlayer) {
        SetDoorOpen(!isOpen);
    }
}