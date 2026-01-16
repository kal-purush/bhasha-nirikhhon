using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraController : MonoBehaviour
{

    [SerializeField] private Player player;

    [SerializeField] private Vector2 cameraSize = new Vector2(20f, 11.25f);

    private bool _lockXAxis;
    private bool _lockYAxis;

    private bool _previouslyLockedX;
    private bool _previouslyLockedY;

    [SerializeField] private float lockedX = 0;
    [SerializeField] private float lockedY = 0;

    [SerializeField] private float moveSpdX = 1.0f;
    [SerializeField] private float moveSpdY = 1.0f;

    [SerializeField] private Vector2 playerRelativeTargetPos = new Vector2(0, 0);
    [SerializeField] private Vector2 playerRelativePreviousPos = new Vector2(0, 0);
    //interpolators
    [SerializeField] private float tX = 0;
    [SerializeField] private float tY = 0;

    [SerializeField] private Vector3 playerRelativePos = new Vector3();
    [SerializeField] private Vector3 globalPos = new Vector3();

    [SerializeField] private float lookDistanceX = 2.0f;
    [SerializeField] private float jumpOffsetY = 1.0f;

    // Update is called once per frame
    void FixedUpdate()
    {

        if(tX < 1.0f) tX += moveSpdX * Time.deltaTime;
        if(tY < 1.0f) tY += moveSpdY * Time.deltaTime;

        if(!_lockXAxis) FreeXMovement(); else LockedXMovement();
        if(!_lockYAxis) FreeYMovement(); else LockedYMovement();
        
        playerRelativePos = new Vector3(playerRelativePos.x, playerRelativePos.y, -10);
        
        CheckRoom();
    }

    private void FreeXMovement()
    {
        float newPlayerRelativeTargetPosX;
        if (player.playerMovement.playerRB.velocity.x == 0)
        {
            newPlayerRelativeTargetPosX = lookDistanceX * (player.playerMovement.isLeft ? -1 : 1);
        }
        else
        {
            newPlayerRelativeTargetPosX = 0;
        }

        if (newPlayerRelativeTargetPosX != playerRelativeTargetPos.x || _previouslyLockedX)
        {
            _previouslyLockedX = false;
            tX = 0;
            playerRelativePreviousPos.x = transform.position.x - player.transform.position.x;
            playerRelativeTargetPos.x = newPlayerRelativeTargetPosX;
        }
        
        playerRelativePos.x = Mathf.Lerp(playerRelativePreviousPos.x, playerRelativeTargetPos.x, tX);
        transform.position = new Vector3(player.transform.position.x + playerRelativePos.x, transform.position.y, transform.position.z);
    }

    private void FreeYMovement()
    {
        float newPlayerRelativeTargetPosY;
        if (player.playerMovement.playerRB.velocity.y > 0)
        {
            newPlayerRelativeTargetPosY = -jumpOffsetY;
        }
        else
        {
            newPlayerRelativeTargetPosY = 0;
        }
        
        if (newPlayerRelativeTargetPosY != playerRelativeTargetPos.y || _previouslyLockedY)
        {
            _previouslyLockedY = false;
            tY = 0;
            playerRelativePreviousPos.y = transform.position.y - player.transform.position.y;
            playerRelativeTargetPos.y = newPlayerRelativeTargetPosY;
        }
        
        playerRelativePos.y = Mathf.Lerp(playerRelativePreviousPos.y, playerRelativeTargetPos.y, tY);
        transform.position = new Vector3(transform.position.x, player.transform.position.y + playerRelativePos.y, transform.position.z);
    }

    private void LockedXMovement()
    {
        var newLockedX = player.room.RoomBounds.center.x;
        
        if (newLockedX != lockedX || !_previouslyLockedX)
        {
            _previouslyLockedX = true;
            tX = 0;
            globalPos.x = transform.position.x;
            lockedX = newLockedX;
        }
        
        var posX = Mathf.Lerp(globalPos.x, lockedX, tX);
        transform.position = new Vector3(posX, transform.position.y, transform.position.z);
    }
    
    private void LockedYMovement()
    {
        var newLockedY = player.room.RoomBounds.center.y;
        
        if (newLockedY != lockedY || !_previouslyLockedY)
        {
            _previouslyLockedY = true;
            tY = 0;
            globalPos.y = transform.position.y;
            lockedY = newLockedY;
        }
        
        var posY = Mathf.Lerp(globalPos.y, lockedY, tY);
        transform.position = new Vector3(transform.position.x, posY, transform.position.z);
    }

    private void CheckRoom()
    {
        if (player.room)
        {
            _lockXAxis = player.room.RoomBounds.size.x <= cameraSize.x;
            _lockYAxis = player.room.RoomBounds.size.y <= cameraSize.y;
        }
    }
}