using System.Collections;
using UniRx;
using UniRx.Triggers;
using System.Collections.Generic;
using UnityEngine;

public class Teleport : MonoBehaviour {

    public ViveInput viveInput;

    public Transform cameraRigTransform;
    public GameObject teleportReticlePrefab;
    private GameObject reticle;
    private Transform teleportReticleTransform;

    public Transform headTransform;
    public Vector3 teleportReticleOffset;
    public LayerMask teleportMask;
    private bool shouldTeleport = false;
    private SteamVR_TrackedObject trackedObj;
    public GameObject laserPrefab;
    private GameObject laser;
    private Transform laserTransform;
    private Vector3 hitPoint;

    void Awake()
    {
        trackedObj = GetComponent<SteamVR_TrackedObject>();
    }

    // Use this for initialization
    void Start () {
        viveInput.applicationButton.Subscribe(_ => 
        {
            TeleportPointer();
        }).AddTo(this);

        viveInput.applicationButtonUp.Subscribe(_ =>
        {
            TeleportPlayer();
        }).AddTo(this);


        laser = Instantiate(laserPrefab);
        laserTransform = laser.transform;
	}
	
	// Update is called once per frame
	void Update () {

	}

    // Teleport the player
    void TeleportPlayer()
    {
        if (!shouldTeleport)
        {
            return;
        } else
        {
            Vector3 difference = cameraRigTransform.position - headTransform.position;
            difference.y = 0;
            cameraRigTransform.position = hitPoint + difference;

            laser.SetActive(false);
            shouldTeleport = false;
            //reticle.SetActive(false);
        }
    }

    // Update where intended teleport location is
    void TeleportPointer()
    {
        RaycastHit hit;
        if (Physics.Raycast(trackedObj.transform.position, transform.forward, out hit, 100))
        {
            hitPoint = hit.point;
            ShowLaser(hit);
            shouldTeleport = true;
            //reticle.SetActive(true);
        } else
        {
            laser.SetActive(false);
            shouldTeleport = false;
            //reticle.SetActive(false);
        }
    }

    private void ShowLaser(RaycastHit hit)
    {
        laser.SetActive(true);
        laserTransform.position = Vector3.Lerp(trackedObj.transform.position, hitPoint, .5f);
        laserTransform.LookAt(hitPoint);
        laserTransform.localScale = new Vector3(laserTransform.localScale.x, laserTransform.localScale.y, hit.distance);

        //teleportReticleTransform.position = hitPoint + teleportReticleOffset;
    }
}