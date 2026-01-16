using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ArcherBowArmAttack : Photon.MonoBehaviour {
    Animator anim;
    bool bowReady = false;

    private int cameraSpeed = 6;
    private GameObject bow;

    [PunRPC]
    public void spawnBow()
    {
        bow = (GameObject)Instantiate(Resources.Load("ArcherBow"));
        bow.transform.parent = transform;
        bow.transform.position = transform.Find("BowPos").transform.position;
        bow.transform.rotation = transform.Find("BowPos").transform.rotation;
    }

    [PunRPC]
    public void fireBow(Vector3 direction)
    {
        anim.CrossFade("Arm", 1.0f, -1, 0f);
        bow.GetComponent<Rigidbody>().useGravity = true;
        bow.GetComponent<Rigidbody>().isKinematic = false;
        bow.transform.parent = null;
        bow.transform.LookAt(direction);
        bow.GetComponent<Rigidbody>().AddRelativeForce(new Vector3(0, 0, 600f));
    }

    [PunRPC]
    public void armBow(bool param)
    {
        if (anim != null)
        {
            anim.SetBool("Arm", param);
        }
    }

    [PunRPC]
    public void destroyBow()
    {
        Debug.Log("Destroying bow");
        Destroy(bow);
    }

    // Use this for initialization
    void Start () {
        anim = GetComponent<Animator>();
    }
	
	// Update is called once per frame
	void Update () {
        if (!photonView.isMine) return;

        GetComponent<WSAD>().enabled = !bowReady;

        if (Input.GetMouseButton(1)) {
            photonView.RPC("armBow", PhotonTargets.All, true);

            if (bowReady && Input.GetMouseButtonDown(0))
            {
                bowReady = false;
                photonView.RPC("fireBow", PhotonTargets.All, Camera.main.ViewportToWorldPoint(new Vector3(0.5f, 0.5f, 10)));
            }
            Camera.main.transform.position = Vector3.MoveTowards(Camera.main.transform.position, transform.Find("CameraPosAim").position, cameraSpeed * Time.deltaTime);
        } else
        {
            if (bowReady) photonView.RPC("destroyBow", PhotonTargets.All);
            bowReady = false;
            photonView.RPC("armBow", PhotonTargets.All, false);
            Camera.main.transform.position = Vector3.MoveTowards(Camera.main.transform.position, transform.Find("CameraPos").position, cameraSpeed * Time.deltaTime);
        }
	}

    // Anim-triggered bow spawn
    void OnBowReady()
    {
        if (!photonView.isMine) return;
        bowReady = true;
        photonView.RPC("spawnBow", PhotonTargets.All);
    }
}