using UnityEngine;
using System.Collections;

public class MyTeleport : MonoBehaviour
{
    [SerializeField] GameObject viveRig;
    [SerializeField] GameObject viveEye;
    [SerializeField] GameObject teleportReticle;

    SteamVR_TrackedController leftControl;
    SteamVR_TrackedController rightControl;

    Vector3 rayCastPos = new Vector3(0,0,0);

    bool castingTeleport = false;

    // Debug var ///////////////
    bool onTarget = false;
    ////////////////////////////

	// Use this for initialization
	void Start ()
    {
        leftControl  = GameObject.FindObjectOfType<SteamVR_ControllerManager>().left.GetComponent<SteamVR_TrackedController>();
        rightControl = GameObject.FindObjectOfType<SteamVR_ControllerManager>().right.GetComponent<SteamVR_TrackedController>();
    }
	

	void FixedUpdate()
    {
        if(leftControl.padPressed && !castingTeleport)
        {
            StartCoroutine(Teleport(leftControl));
        }

        if (rightControl.padPressed && !castingTeleport)
        {
            StartCoroutine(Teleport(rightControl));
        }
    }

    IEnumerator Teleport(SteamVR_TrackedController teleportingHand)
    {
        castingTeleport = true;

        Vector3 targetPos = Vector3.zero;
        bool havePos = false;

        while (teleportingHand.padPressed)
        {
            RaycastHit hit;
            //Ray ray = new Ray(teleportingHand.gameObject.transform.position, teleportingHand.gameObject.transform.forward, 100);

            if (Physics.Raycast(teleportingHand.gameObject.transform.position, teleportingHand.gameObject.transform.forward, out hit, 1000))
            {
                if (hit.collider != null)
                {
                    if (hit.collider.gameObject.CompareTag("TeleportTarget"))
                    {
                        targetPos = hit.point;
                        //Debug.DrawLine(teleportingHand.gameObject.transform.position, hit.point, Color.red);
                        havePos = true;
                    }
                    else
                    {
                        havePos = false;
                    }
                }

                onTarget = havePos;
            }

            else
            {
                havePos = false;
            }

            
            teleportReticle.gameObject.SetActive(havePos);
            teleportReticle.transform.position = targetPos;
            
            yield return null;
        }

        if (havePos)
            viveRig.transform.position = targetPos;

        teleportReticle.SetActive(false);
        castingTeleport = false;
    }
}