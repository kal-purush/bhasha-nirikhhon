using UnityEngine;
using System.Collections;

public class PlayerCameraCreator : MonoBehaviour {

    public Vector3 cameraOffset;
    public Vector3 cameraRot;

    GameObject PlayerCam;

	void Start () {
        
        PlayerCam = new GameObject();
        PlayerCam.AddComponent<Camera>();
        PlayerCam.AddComponent<AudioListener>();
        PlayerCam.name = "PlayerCamera";
	}

    void LateUpdate()
    {
        PlayerCam.transform.position = gameObject.transform.position + cameraOffset;
        PlayerCam.transform.rotation = Quaternion.Euler(cameraRot.x, cameraRot.y, cameraRot.z);
    }
}