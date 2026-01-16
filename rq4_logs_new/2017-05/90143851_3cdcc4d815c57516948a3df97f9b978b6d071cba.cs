using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LaserPointer : MonoBehaviour {
	
	// after Tutorial: https://www.raywenderlich.com/149239/htc-vive-tutorial-unity

	private SteamVR_TrackedObject trackedObj;

	// 1 - reference to Lasers Prefab
	public GameObject laserPrefab;
	// 2 - stores reference to instance of the laser
	private GameObject laser;
	// 3 - transform component is stored for ease of use
	private Transform laserTransform;
	// 4 - position where the laser hits
	private Vector3 hitPoint; 

	private SteamVR_Controller.Device Controller
	{
		get { return SteamVR_Controller.Input((int)trackedObj.index); }
	}

	void Awake()
	{
		trackedObj = GetComponent<SteamVR_TrackedObject>();
	}

	private void ShowLaser(RaycastHit hit)
	{
		// 1 - show laser
		laser.SetActive(true);
		// 2 - position laser between controller and the point where the raycast hits
		laserTransform.position = Vector3.Lerp(trackedObj.transform.position, hitPoint, .5f);
		// 3 - point the laser at the position where the raycast hit
		laserTransform.LookAt(hitPoint); 
		// 4 - scale laser
		laserTransform.localScale = new Vector3(laserTransform.localScale.x, laserTransform.localScale.y,
			hit.distance);
	}

	// Use this for initialization
	void Start () {
		// 1 - spawn new laser and save reference to laser
		laser = Instantiate(laserPrefab);
		// 2 - store laser's transform
		laserTransform = laser.transform;
	}
	
	// Update is called once per frame
	void Update () {
		// 1 - touchpad down
		if (Controller.GetPress(SteamVR_Controller.ButtonMask.Touchpad))
		{
			RaycastHit hit;

			// 2 - shoot ray from controller
			if (Physics.Raycast(trackedObj.transform.position, transform.forward, out hit, 100))
			{
				hitPoint = hit.point;
				ShowLaser(hit);
			}
		}
		else // 3 - on touchpas release hide laser
		{
			laser.SetActive(false);
		}
	}
}