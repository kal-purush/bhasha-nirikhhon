using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Interactable : MonoBehaviour
{
    private bool _isInTrigger;
    public bool _isInteractable = true;

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.F) && _isInTrigger && _isInteractable)
        {
            Interact();
        }
    }

    public virtual void Interact()
    {
        Debug.Log("Interact");
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.gameObject == PlayerManager.Instance.Player)
        {
            _isInTrigger = true;
        }
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.gameObject == PlayerManager.Instance.Player)
        {
            _isInTrigger = false;
        }
    }

    
}