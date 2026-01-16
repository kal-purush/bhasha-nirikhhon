using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraMovement : MonoBehaviour {

    public float scrollSpeed;
    public int scrollDist;

    public int maxX, maxY;

    private void Start()
    {
        maxX = (int)(Board._board.gridWorldSize.x/2);
        maxY = (int)(Board._board.gridWorldSize.y/2);
    }
    private void Update()
    {
        var mousePosX = Input.mousePosition.x;
        var mousePosY = Input.mousePosition.y;
        var posX = transform.position.x;
        var posY = transform.position.z;        

        if (posX < maxX && posX > -maxX && posY < maxY && posY > -maxY)
        {
            if (mousePosX < scrollDist)
                transform.Translate(Vector3.right * -scrollSpeed * Time.deltaTime);
            if (mousePosX >= Screen.width - scrollDist)
                transform.Translate(Vector3.right * scrollSpeed * Time.deltaTime);


            if (mousePosY < scrollDist)
                transform.Translate(Vector3.up * -scrollSpeed * Time.deltaTime);
            if (mousePosY >= Screen.height - scrollDist)
                transform.Translate(Vector3.up * scrollSpeed * Time.deltaTime);
        }

        if (posX > maxX)
            transform.Translate(Vector3.left * .5f * Time.deltaTime);
        if (posX < -maxX)
            transform.Translate(Vector3.right * .5f * Time.deltaTime);
        if (posY > maxY)
            transform.Translate(Vector3.down * 1.5f * Time.deltaTime);
        if (posY < -maxY)
            transform.Translate(Vector3.up * 1.5f * Time.deltaTime);
    }
}