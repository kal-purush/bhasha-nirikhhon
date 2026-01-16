using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GrapplePoint : MonoBehaviour
{
    private OmnicatLabs.CharacterControllers.CharacterController player;

    private void Start()
    {
        player = OmnicatLabs.CharacterControllers.CharacterController.Instance;
    }

    private void Update()
    {
        if (Vector3.Distance(player.transform.position, transform.position) <= player.maxGrappleDistance)
        {
            if (Physics.Raycast(transform.position, (transform.position - player.transform.position).normalized))
            {
                Debug.Log("Active");
            }
        }
    }
}