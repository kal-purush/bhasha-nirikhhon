using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Player : MonoBehaviour
{

    [SerializeField] float moveSpeed;
    [SerializeField] float jumpSpeed;
    [SerializeField] float gravity;

    private Vector3 charaMove;
    bool isGround;
    new Rigidbody rigidbody;
    CharacterController characterController;


    // Start is called before the first frame update
    void Start()
    {
        rigidbody = GetComponent<Rigidbody>();
        characterController = GetComponent<CharacterController>();
        charaMove = Vector3.zero;
        isGround = true;
    }

    // Update is called once per frame
    void Update()
    {
       float horizontal = Input.GetAxis("Horizontal");

        charaMove.x = horizontal * moveSpeed;

        if (characterController.isGrounded)
        {
            if (Input.GetButtonDown("Jump"))
            {
                charaMove.y = jumpSpeed;
            }
            charaMove.y += 0;
        }
        else
        {
            charaMove.y += Physics.gravity.y * gravity * Time.deltaTime;
        }
        characterController.Move(charaMove * Time.deltaTime);
        Debug.Log("charaMove : " + charaMove);
    }

    //private void FixedUpdate()
    //{
    //    float horizontal = Input.GetAxis("Horizontal");
    //    //Debug.Log("horizontal  = " + horizontal);
    //    charaMove.x = horizontal * moveSpeed;
    //    if (characterController.isGrounded)
    //    {
    //        if (Input.GetButtonDown("Jump"))
    //        {
    //            charaMove.y -= Physics.gravity.y * gravity * Time.fixedDeltaTime;
    //            Debug.Log("Jump");
    //        }
    //        charaMove.y = 0;
    //    }
    //    else
    //    {
    //        Debug.Log("horizontal  = ");
    //        charaMove.y += Physics.gravity.y * gravity * Time.fixedDeltaTime;
    //    }
    //    characterController.Move(charaMove * Time.fixedDeltaTime);
    //    Debug.Log("isGrounded" + characterController.isGrounded);
    //}

}