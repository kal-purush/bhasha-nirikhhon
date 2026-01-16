using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using Unity.VisualScripting;
using UnityEngine;

public class CharacterControllerEnabler : MonoBehaviour
{
    public bool EnableController;
    [SerializeField] private PlayerController playerController;
    [SerializeField] private CharacterController CharController;
    [SerializeField] private Rigidbody CharRigidbody;
    
    public IEnumerator EnableCharacterController()
    {
        yield return new WaitForSeconds(0.2f);
        EnableController = true;
        Debug.Log("booltrue");
    }
    
    private void OnCollisionEnter(Collision collision)
    {
        Debug.Log("checking for collision");
        if (EnableController == true)
        {
            Debug.Log("controller bool true");
            if (collision.gameObject.CompareTag("Ground"))
            {
                Debug.Log("enable things and disable bool");
                CharController.enabled = true;
                CharRigidbody.isKinematic = true;
                EnableController = false;
            }
        }
    }
}