using UnityEngine;
using System.Collections;

public class ChangeCameraForDemo : MonoBehaviour {

    [SerializeField] Camera mainCam, camDemo;
    private int countCam = 1;

    // Use this for initialization
    void Start () {
        mainCam.enabled = true;
        camDemo.enabled = false;
	}
	
	// Update is called once per frame
	void Update () {

        if (Input.GetKeyDown(KeyCode.C))
            countCam++;

        if (countCam > 2)
            countCam = 1;

        ChangeCam();
    }

    void ChangeCam()
    {
        switch (countCam)
        {
            case 1:
                mainCam.enabled = true;
                camDemo.enabled = false;
                break;
            case 2:
                mainCam.enabled = false;
                camDemo.enabled = true;
                break;
        }
    }

}