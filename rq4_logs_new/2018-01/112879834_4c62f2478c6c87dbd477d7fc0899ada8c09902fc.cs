using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// Simple class to control minion movement.
/// The only thing this class does is lerp between nodes over a set interval.
/// Destruction of minions and handling what happens when we reach the origin
/// are handled elsewhere.
/// Only corner case is not crashing the game when node.OutNode is null (we should just stop moving).
/// </summary>
public class MovementController : MonoBehaviour
{
    public const float LEFT_ROT = 0;
    public const float UP_ROT = 90f;
    public const float RIGHT_ROT = 180f;
    public const float DOWN_ROT = 270f;

    public Node Location { get; set; }
    public bool Active { get; private set; }

    public float MoveInterval { get; set; }
    public float TurnInterval { get; set; }

	// Use this for initialization
	public void Start ()
    {
        Active = false;
	}
	
	// Update is called once per frame
	public void Update ()
    {
        if (Active)
        {

        }
	}

    /// <summary>
    /// Activate this minion.
    /// All this does is begin moving down the path and animating the walk cycle.
    /// It's intended to be called right after instantiation, and is basically here to
    /// give us time to initialize the minion.
    /// Also does some light lifting, like moving the minion automatically to the passed node location.
    /// </summary>
    /// <param name="location"></param>
    public void Activate(Node location)
    {
        float minionRot = LEFT_ROT;

        // Get Direction
        switch(location.OutDirection)
        {
            case NODE_DIRECTION.UP:
                minionRot = UP_ROT;
                break;
            case NODE_DIRECTION.RIGHT:
                minionRot = RIGHT_ROT;
                break;
            case NODE_DIRECTION.DOWN:
                minionRot = DOWN_ROT;
                break;
            default:
                throw new MovementControllerException(MOVEMENT_CONTROLLER_EXCEPTION_TYPES.BAD_ACTIVATION_DIRECTION);
        }

        transform.SetPositionAndRotation(location.VectorLocation.ToVector3(), Quaternion.Euler(0, minionRot, 0));
    }
}

public enum MOVEMENT_CONTROLLER_EXCEPTION_TYPES
{
    BAD_ACTIVATION_DIRECTION
}

public class MovementControllerException : Exception
{
    private static string GetMessage(MOVEMENT_CONTROLLER_EXCEPTION_TYPES type)
    {
        switch (type)
        {
            case MOVEMENT_CONTROLLER_EXCEPTION_TYPES.BAD_ACTIVATION_DIRECTION:
                return "Found a node with a multi-direction out. That shouldn't happen yet.";
            default:
                return string.Empty;
        }
    }

    public MovementControllerException(MOVEMENT_CONTROLLER_EXCEPTION_TYPES type) : base(GetMessage(type)) { }
}