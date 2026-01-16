using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerState{

    // Property
    public bool OnGround { get { return _onGround; } }
    public bool IsJump { get { return _isJump; } }
    public bool IsStanding { get { return _isStanding; } }
    public bool IsWalking { get { return _isWalking; } }

    private bool _onGround;
    private bool _isJump;
    private bool _isStanding;
    private bool _isWalking;
    
    // Constructor
    public PlayerState()
    {
        _onGround = false;
        _isJump = false;
        _isStanding = true;
        _isWalking = false;
    }

    public void Jump()
    {
        _onGround = false;
        _isJump = true;
        _isStanding = false;
        _isWalking = false;
    }

    public void LandGround()
    {
        _onGround = true;
        _isStanding = true;
        _isJump = false;
    }

    public void Walking()
    {
        _isStanding = false;
        _isWalking = true;
    }

    public void Standing()
    {
        _isStanding = true;
        _isWalking = false;
    }
}