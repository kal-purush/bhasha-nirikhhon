using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ratScript : MonoBehaviour
{
    private Rigidbody body;

    public float torqueConstant;
    public float forceConstant;

    public enum ratStates
    {
        statePatrol
    }

    public ratStates currentState;
    
    public GameObject pathContainer;
    private List<Transform> path;
    int targetNode = 0;

    private void Awake()
    {
        body = GetComponent<Rigidbody>();
        // if the rat has a path
        if (pathContainer != null)
        {
            // fetch the path
            path = pathContainer.GetComponent<PathManagerScipt>().nodes;
        }
    }

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {

        // state machine
        switch (currentState)
        {
            case ratStates.statePatrol:
                if (!MoveTowardsPosXZ(GetTargetNodePosition())) // if we're already at that point
                {
                    // target the next node
                    targetNode++;
                    if (targetNode >= path.Count) // if the next node does not exist
                    {
                        // target the first node
                        targetNode = 0;
                    }
                }
                break;
            default:
                break;
        }


        
    }

    // This function will rotate the rat to face the position and the push the rat towards the point
    // Returns false if the rat is already at the position.
    private bool MoveTowardsPosXZ(Vector3 point)
    {
        bool returnValue = true;
        Vector2 pointXZ = new Vector2(point.x, point.z);

        Vector2 ratPositionXZ = new Vector2(transform.position.x, transform.position.z);
        Vector2 ratDirectionXZ = new Vector2(transform.forward.x, transform.forward.z);

        Vector2 direction = pointXZ - ratPositionXZ;

        Vector2 directionNormalized = direction.normalized;
        Vector2 ratDirectionXZnormalized = ratDirectionXZ.normalized;
        if (pointXZ != ratPositionXZ)
        {
            // if the rat is not facing the point
            if (directionNormalized != ratDirectionXZnormalized)
            {
                // if the two are very similar, set the rotation and kill the angular velocity
                bool directionSimilar = false;
                // if x is similar
                if (directionNormalized.x > ratDirectionXZnormalized.x - 0.1F
                    && directionNormalized.x < ratDirectionXZnormalized.x + 0.1F)
                {
                    // if z is similar (registers as y since it's a Vector2)
                    if (directionNormalized.y > ratDirectionXZnormalized.y - 0.1F
                        && directionNormalized.y < ratDirectionXZnormalized.y + 0.1F)
                    {
                        directionSimilar = true;
                    }
                }
                if (directionSimilar)
                {
                    Vector3 temp = transform.forward;
                    temp.x = direction.x;
                    // once again, registers as y since Vector2
                    temp.z = direction.y;
                    transform.forward = temp;
                    body.angularVelocity = Vector3.zero;
                }
                else // direction is not similar.
                {
                    // starts with anti-clockwise rotation
                    float rotationDirection = -1.0F;
                    if (directionNormalized.x >= ratDirectionXZnormalized.x)
                    {
                        // sets the rotation to be clockwise
                        rotationDirection = 1.0F;
                    }

                    // we need to rotate the rat towards the direciton
                    Vector3 torque = transform.up * torqueConstant * rotationDirection * Time.deltaTime;
                    body.AddTorque(torque);
                }
            }
            else // the rat is facing the point
            {
                float distance = direction.magnitude;
                if (distance > 0.1F) // we aren't very close to the target
                {
                    body.AddForce(transform.forward * forceConstant * Time.deltaTime);
                }
                else // we are very close <3
                {
                    Vector3 temp = transform.position;
                    temp.x = point.x;
                    temp.z = point.z;
                    transform.position = temp;
                    body.velocity = Vector3.zero;
                }
            }
            
        }
        else
        {
            returnValue = false;
        }
        
        return returnValue;
    }

    private Vector3 GetTargetNodePosition()
    {
        return path[targetNode].position;
    }
}