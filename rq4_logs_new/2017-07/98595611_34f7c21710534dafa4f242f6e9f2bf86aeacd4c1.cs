using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Grapple : MonoBehaviour {

    private Transform player;
    public LayerMask canGrapple;
    public float grappleSpeed = 20;
    public LineRenderer lineRenderer;
    bool isFlying = false;
    Vector3 grapplePoint;

	// Use this for initialization
	void Start () {
        player = GameObject.FindGameObjectWithTag("Player").transform;
	}
	
	// Update is called once per frame
	void Update () {
        if (Input.GetButtonDown("Fire1") && !isFlying)
        {
            Ray ray = new Ray(Camera.main.transform.position, Camera.main.transform.forward);
            RaycastHit hit;
            if (Physics.Raycast(ray, out hit, 1000))
            {
                if (hit.transform.gameObject.CompareTag("Hookshot"))
                {
                    isFlying = true;
                    grapplePoint = hit.point;
                    lineRenderer.enabled = true;
                    lineRenderer.SetPosition(1, grapplePoint);
                    player.GetComponent<FirstPersonDrifter>().canMove = false;
                }
            }
        }

        if (isFlying)
        {
            Flying();
        }

        if (Input.GetButtonDown("Jump") && isFlying)
        {
            isFlying = false;
            lineRenderer.enabled = false;
            player.GetComponent<FirstPersonDrifter>().canMove = true;
        }

        if (Input.GetButtonUp("Fire1"))
        {

        }

    }


    void OnDisable()
    {
        isFlying = false;
        lineRenderer.enabled = false;
        player.GetComponent<FirstPersonDrifter>().canMove = true;
    }


    public void Flying()
    {
        player.transform.position = Vector3.Lerp(player.transform.position, grapplePoint, grappleSpeed * Time.deltaTime / Vector3.Distance(player.transform.position, grapplePoint));
        lineRenderer.SetPosition(0, transform.position);

        if (Vector3.Distance(player.transform.position, grapplePoint) < 0.5f)
        {
            isFlying = false;
            lineRenderer.enabled = false;
            player.GetComponent<FirstPersonDrifter>().canMove = true;

        }
    }

}