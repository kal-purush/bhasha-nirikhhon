using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CrossHairMovement : MonoBehaviour
{
    public PlayerController mainShip;
    public PlayerController rightShip;
    public PlayerController leftShip;
    public GameBehavior game;
    public CharacterController controller;
    public float speed;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        //Variable to hold mouse position
        Vector3 mousePos = Input.mousePosition;

        //Use the current camera to convert mouse position to a ray
        Ray mouseRay = Camera.main.ScreenPointToRay(mousePos);

        //Create a plane that faces up at the same position as the player
        Plane playerPosPlane = new Plane(Vector3.forward, transform.position);

        //How far along the ray does the intersection with the plan occur?
        float rayDistance = 0;
        playerPosPlane.Raycast(mouseRay, out rayDistance);

        //Use the ray distance to calculate the point of collision
        Vector3 targetPoint = mouseRay.GetPoint(rayDistance);

        //Move agent to target point
        //transform.position = new Vector3(targetPoint.x, -8.0f, 0.0f);
        Vector3 direction = targetPoint - transform.position;
        if(direction.magnitude <= 0.5)
        {
            return;
        }
        direction.Normalize();
        controller.Move(direction * speed * Time.deltaTime);

        mainShip.transform.position = new Vector3(transform.position.x, mainShip.transform.position.y, mainShip.transform.position.z);
        leftShip.transform.position = new Vector3(leftShip.transform.position.x, transform.position.y, leftShip.transform.position.z);
        rightShip.transform.position = new Vector3(rightShip.transform.position.x, transform.position.y, rightShip.transform.position.z);
    }
}