using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class PlayerControllerScript2 : MonoBehaviour
{
    public bool debugMode = true;
    public float maxPlayerDistance = 5f;

    [SerializeField] float playerOneMoveSpeed = 10f;
    [SerializeField] GameObject playerOneObject;
    [SerializeField] GameObject playerOneModel;
    private Rigidbody playerOneRB;
    private Vector2 moveInputOne = Vector2.zero;

    [SerializeField] float playerTwoMoveSpeed = 10f;
    [SerializeField] GameObject playerTwoObject;
    [SerializeField] GameObject playerTwoModel;
    private Rigidbody playerTwoRB;
    private Vector2 moveInputTwo = Vector2.zero;

    private Keyboard myKB;
    private Gamepad myGPOne;
    //private Gamepad myGPTwo;
    private bool GPControlsOn = true;

    
    [SerializeField] GameObject centerObject;
    [SerializeField] GameObject tooFarUI;
    public static bool tooFarBool = false;

    private void Awake()
    {
        myKB = Keyboard.current;
        myGPOne = Gamepad.current;
        if (myGPOne == null)
        {
            GPControlsOn = false;
            return;
        }
    }

    // Start is called before the first frame update
    void Start()
    {
        tooFarUI.SetActive(false);
        playerOneRB = playerOneObject.GetComponent<Rigidbody>();
        playerTwoRB = playerTwoObject.GetComponent<Rigidbody>();
        centerObject.transform.localScale = new Vector3(maxPlayerDistance, maxPlayerDistance, maxPlayerDistance);
    }

    // Update is called once per frame
    void Update()
    {



        if (debugMode)
        {
            if (tooFarBool)
            {
                tooFarUI.SetActive(true);
            }
        }
        

        if(GPControlsOn)
        {
            // GP controls
        }
        else // KB controls
        {
            ////////////////
            // Player One //
            ////////////////

            //left-right
            if (myKB.dKey.isPressed)
            {
                moveInputOne.x = 1f;
            }
            else if (myKB.aKey.isPressed)
            {
                moveInputOne.x = -1f;
            }
            else
            {
                moveInputOne.x = 0f;
            }
            //forward-back
            if (myKB.wKey.isPressed)
            {
                moveInputOne.y = 1f;
            }
            else if (myKB.sKey.isPressed)
            {
                moveInputOne.y = -1f;
            }
            else
            {
                moveInputOne.y = 0f;
            }



            ////////////////
            // Player Two //
            ////////////////

            //left-right
            if (myKB.lKey.isPressed)
            {
                moveInputTwo.x = 1f;
            }
            else if (myKB.jKey.isPressed)
            {
                moveInputTwo.x = -1f;
            }
            else
            {
                moveInputTwo.x = 0f;
            }
            //forward-back
            if (myKB.iKey.isPressed)
            {
                moveInputTwo.y = 1f;
            }
            else if (myKB.kKey.isPressed)
            {
                moveInputTwo.y = -1f;
            }
            else
            {
                moveInputTwo.y = 0f;
            }



        }
    }

    private void FixedUpdate()
    {
        CheckPlayerDistance(maxPlayerDistance);
        PlayerOneLocomotion();
        PlayerTwoLocomotion();
    }
    private void LateUpdate()
    {
        if (myKB.escapeKey.wasPressedThisFrame)
        {
            
        }
    }

    private void PlayerOneLocomotion()
    {
        //////////////
        // player 1 //
        //////////////

        playerOneModel.transform.localPosition = Vector3.zero;
        Vector3 playerOneMoveVector = new Vector3(moveInputOne.x, 0f, moveInputOne.y);
        playerOneRB.velocity = playerOneMoveVector * playerOneMoveSpeed;

        if (playerOneMoveVector != Vector3.zero)
        {
            Quaternion targetRotation = Quaternion.LookRotation(playerOneMoveVector);
            targetRotation = Quaternion.RotateTowards(playerOneModel.transform.rotation, targetRotation, 360 * Time.fixedDeltaTime);
            playerOneModel.GetComponent<Rigidbody>().MoveRotation(targetRotation);
        }
    }

    private void PlayerTwoLocomotion()
    {
        //////////////
        // player 2 //
        //////////////

        playerTwoModel.transform.localPosition = Vector3.zero;
        Vector3 playerTwoMoveVector = new Vector3(moveInputTwo.x, 0f, moveInputTwo.y);
        playerTwoRB.velocity = playerTwoMoveVector * playerTwoMoveSpeed;

        if (playerTwoMoveVector != Vector3.zero)
        {
            Quaternion targetRotation = Quaternion.LookRotation(playerTwoMoveVector);
            targetRotation = Quaternion.RotateTowards(playerTwoModel.transform.rotation, targetRotation, 360 * Time.fixedDeltaTime);
            playerTwoModel.GetComponent<Rigidbody>().MoveRotation(targetRotation);
        }
    }

    private void CheckPlayerDistance(float maxDistance)
    {
        //float currentDistance = Vector3.Distance(playerOneObject.transform.position, playerTwoObject.transform.position);
        /*
        Vector3 directionCentertoOne = playerOneObject.transform.position - centerObject.transform.position;
        Vector3 directionCentertoTwo = playerTwoObject.transform.position - centerObject.transform.position;
        Vector3 midpointOneToTwo = new Vector3((directionCentertoOne.x + directionCentertoTwo.x) / 2.0f, (directionCentertoOne.y + directionCentertoTwo.y) / 2.0f, (directionCentertoOne.z + directionCentertoTwo.z) / 2.0f);
        */

        Vector3 midpointOneToTwo = ((playerOneObject.transform.position - playerTwoObject.transform.position) * 0.5f) + playerTwoObject.transform.position;

        centerObject.transform.position = midpointOneToTwo;


        

    }
}