using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class teleportScript : MonoBehaviour
{
    // Start is called before the first frame update

    bool movingTowards = true;
    public GameObject thisPortal;
    public GameObject targetPortal;
    public GameObject playerReference;
    public float angleBy = 0;
    public bool readyToTeleport = true;
    private float distanceToPlayer = 0;
    private float teleportThreshold = 3;
    private BoxCollider thisCollider;


    void start()
    {
        thisCollider = this.GetComponent<BoxCollider>();
    }

    void OnTriggerEnter(Collider other)
    {
       

        if (other.name == "Capsule" && readyToTeleport)
        {
            //set player position
            PlayerMovement playerScript = other.GetComponent<PlayerMovement>();
            Vector3 portalOffset = Quaternion.AngleAxis(angleBy,Vector3.up) * (playerReference.transform.position - this.transform.position);
            playerScript.teleport(targetPortal.transform.position + portalOffset,angleBy);
            //set other portal reference
            teleportScript tpScript =  targetPortal.GetComponent<teleportScript>();
            tpScript.readyToTeleport = false;


        }

    }

    void OnTriggerExit(Collider other)
    {
        if (other.name == "Capsule")
        {
            readyToTeleport = true;
        }
    }
    // Update is called once per frame
    // void Update()
    // {
    //     distanceToPlayer = Vector3.Distance(thisStair.transform.position,playerReference.transform.position);

    //     Vector3 thisforward = this.thisStair.transform.forward;
    //     //Vector3 vect_dist = Quaternion.Euler(0,angleBy,0) * (thisStair.transform.position - playerReference.transform.position);
    //     if((distanceToPlayer < teleportThreshold ) && gracePeriod == false)
    //     {
    //         PlayerMovement playerScript = playerReference.GetComponent<PlayerMovement>();
    //         Vector3 teleportBy =  (targetStair.transform.position - thisStair.transform.position );
    //         playerScript.teleport(teleportBy, angleBy);
            
    //         teleportScript targetScript = targetStair.GetComponent<teleportScript>();
    //         targetScript.setGrace();
            
    //     }
    //     else if (distanceToPlayer > teleportThreshold + 1)
    //     {
    //         gracePeriod = false;
    //     }
    // }

}