using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraMovement : MonoBehaviour {

    private Vector3 offset;
    private GameObject Player;
        private ClickAndDrag EditManager;
    public float DampSpeed;
	// Use this for initialization
	void Start () {
        Player = GameObject.Find("Player");
        EditManager = GameObject.Find("EditManager").GetComponent<ClickAndDrag>();
        offset = transform.position - Player.transform.position;
        CameraController = GetComponent<CharacterController>();
	}

    public bool EditMode;
    private bool buttonpressed;
    void CameraMode()
    {
        switch(EditMode)
        {
            case false: PlayerCameraMovement(); break;
            case true: EditCameraMovement(); break;
        }

        if (EditManager.DMode == true)
        {
            transform.rotation = Quaternion.Lerp(transform.rotation, Quaternion.Euler(90, 0, 0),0.03f);
        }
        if (EditManager.DMode == false)
        {
            transform.rotation = Quaternion.Lerp(transform.rotation, Quaternion.Euler(60, 0, 0), 0.03f);
        }


        if (Input.GetKeyDown(KeyCode.Escape) || Input.GetKeyDown(KeyCode.T) || Input.GetKeyDown(KeyCode.Z))
        {
            buttonpressed = false;
            if (EditMode == false && buttonpressed==false) { EditMode = true;buttonpressed = true;CameraController.enabled = true; }
            else if (EditMode == true && buttonpressed == false) { EditMode = false; buttonpressed = true; CameraController.enabled = false; }
        }
    }

    void PlayerCameraMovement()
    {
        transform.position = Vector3.Lerp(transform.position, Player.transform.position + offset, (DampSpeed/100));
    }




    public float CharacterSpeed;
    Vector3 TravelDirection;
    CharacterController CameraController;
    void EditCameraMovement()
    {
        if (EditManager.DMode == true) { transform.position = Vector3.Lerp(transform.position, new Vector3(transform.position.x, (Player.transform.position.y + (offset.y * 1.4f)), transform.position.z), (DampSpeed / 100)); }
        if (EditManager.DMode == false && EditManager.GridView==true) { transform.position = Vector3.Lerp(transform.position, new Vector3(transform.position.x, (Player.transform.position.y + offset.y), transform.position.z), (DampSpeed / 100)); }
        TravelDirection = new Vector3((Input.GetAxis("Horizontal")), 0, (Input.GetAxis("Vertical")));
        TravelDirection *= CharacterSpeed;
        if (Input.GetKey(KeyCode.LeftShift)) { TravelDirection *= 2; }
        CameraController.Move(TravelDirection * Time.deltaTime);

        
    }





	void LateUpdate () {
        CameraMode();
	}
}