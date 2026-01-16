using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Grapple : MonoBehaviour
{
    [SerializeField]
    float maxDistance;
    float currentDistance;

    [SerializeField]
    BalloonMove player;

    //Add in area around balloon where the grapple can be used where it will travel over a certain distnace and check if it comes in contact with an object via raycasting
    //Once it hits an object - turn off IsKinetatic (so no other forces affect it) and pull the balloon towards that point.
    //Once at a certain distance from the point, turn IsKinematic back on and return hook

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
       
    }

    void FireHook()
    {
        currentDistance = Vector3.Distance(this.transform.position, player.transform.position);

        player.GetComponent<Rigidbody>().isKinematic = false;

        if (currentDistance <= 1)
        {
            player.GetComponent<Rigidbody>().isKinematic = true;
        }
        if (currentDistance > maxDistance)
        {
            ReturnHook();
        }
    }

    void ReturnHook()
    {
        this.transform.position = player.transform.position;
    }

    private void OnTriggerEnter(Collider other)
    {
        
    }
}