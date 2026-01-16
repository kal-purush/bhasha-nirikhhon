using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CarCamera : MonoBehaviour {

	void Start () {
        Camera.main.GetComponent<CameraFollow>().target = transform;
	}
}