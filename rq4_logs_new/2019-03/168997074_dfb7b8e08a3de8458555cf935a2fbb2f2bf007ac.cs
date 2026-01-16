using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Breaker : Interactable
{
    #region Variables
    //Private variables

    //Editor variables
    public Instruction breakerInstruction;
    public List<Light> lights;
    public bool enabled;
    //Piece of the UI

    #endregion

    #region Methods

    //Overriding methods
    public override void PlayerInteract(Player player)
    {
        base.PlayerInteract(player);
        InteractBreaker(player.gameObject);
    }

    //class methods

    /// <summary>
    /// Teleports entity to the exit vent
    /// </summary>
    private void InteractBreaker(GameObject entity)
    {
        if (enabled)
        {
            foreach (Light light in lights)
                light.enabled = false;
            enabled = false;
        }
        else
        {
            foreach (Light light in lights)
                light.enabled = true;
            enabled = true;
        }
    }

    #endregion

    #region Functions

    void Awake()
    {

    }

    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.layer == 9)
        {
            Debug.Log(other.gameObject.name + " entered the breaker range");
            //Start showing the UI
        }
    }

    void OnTriggerStay(Collider other)
    {
        if (other.gameObject.layer == 9)
        {
            if (Input.GetButtonDown("Interact"))
            {
                Debug.Log("Player interacted with the breaker");
                PlayerInteract(other.GetComponent<Player>());
            }
        }
        if (other.gameObject.layer == 10)
        {
            Debug.Log("Entity interacted with the breaker");
            PlayerInteract(other.GetComponent<Player>());
        }
    }

    void OnTriggerExit(Collider other)
    {
        if (other.gameObject.layer == 9)
        {
            Debug.Log(other.gameObject.name + " left the breaker range");
            //Stop showing the UI
        }
    }
    #endregion
}