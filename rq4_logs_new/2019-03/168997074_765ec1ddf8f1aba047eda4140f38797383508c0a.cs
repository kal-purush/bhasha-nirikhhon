using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TopdownController : MonoBehaviour
{

    #region Variables

    public float speed = 3;
    private Vector3 pos;
    #endregion

    #region Functions

    void Update()
    {
        //Movement

        float horizontalInput = Input.GetAxis("Horizontal");
        float verticalInput = Input.GetAxis("Vertical");

        Vector3 inputDirection = new Vector3(horizontalInput, 0, verticalInput).normalized;

        this.gameObject.transform.position += inputDirection * speed * Time.deltaTime;

        //Orientation

        pos = Input.mousePosition;
        pos.z = 15f;
       
        pos = Camera.main.ScreenToWorldPoint(pos);
        Debug.Log(pos);
        Debug.DrawRay(this.transform.position, (new Vector3(pos.x, this.transform.position.y, pos.z) - this.transform.position));
        transform.LookAt(new Vector3(pos.x, this.transform.position.y, pos.z));
    }
    #endregion
}