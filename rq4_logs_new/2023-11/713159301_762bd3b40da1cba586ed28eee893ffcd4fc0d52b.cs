using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Platform : MonoBehaviour
{
    public Vector2 leftBound {get; private set;}
    public Vector2 rightBound { get; private set; }
    private float enemyWidthOffset = 0;
    [HideInInspector] public float EnemyWidthOffset { get { return enemyWidthOffset; } set { enemyWidthOffset = value + edgeBuffer; } }//Enemy will define when it lands on a platform
    public NearbyPlatformData[] platformData;
    BoxCollider2D boxCollider;
    LayerMask ignore;

    public float horizontalDropDistance = 1; //The amount that the enemy moves sideways while dropping down onto a lower platform. Will depend on how big the enemy sprites are. Can edit in inspector if its a close call whether the enemy will land or not
    float edgeBuffer = .1f; //Distance enemies will stand from the edge, this gets added to the width offset

    private void Awake()
    {
        boxCollider = GetComponent<BoxCollider2D>();
        ignore |= (1 << LayerMask.NameToLayer("Environment"));//prevents accidental detection of enemies or other objects while defining platform boundaries. Walls and Platforms must be in the "Environment" layer
        SetBounds();
    }

    private void Start()
    {
        GenerateNearbyPlatformLinks();
    }

    private void SetBounds()
    {
        leftBound = new Vector2((transform.position.x + boxCollider.bounds.extents.x * -1) + enemyWidthOffset , transform.position.y + boxCollider.bounds.extents.y); //Left-most point of the platform
        rightBound = new Vector2((transform.position.x + boxCollider.bounds.extents.x) - enemyWidthOffset, transform.position.y + boxCollider.bounds.extents.y);//Right-most point of the platform

        RaycastHit2D hit;
        hit = Physics2D.Raycast(new Vector2(transform.position.x, transform.position.y + (boxCollider.bounds.extents.y + .25f)), Vector2.left, boxCollider.bounds.extents.x,ignore);//Checks for Walls on the left
        if(hit.collider != null)
        {
            leftBound = new Vector2(hit.point.x + enemyWidthOffset, leftBound.y);
            //print("Collision on Left");
        }

        hit = Physics2D.Raycast(new Vector2(transform.position.x, transform.position.y + (boxCollider.bounds.extents.y + .25f)), Vector2.right, boxCollider.bounds.extents.x, ignore); //Checks for Walls on the Right
        if (hit.collider != null)
        {
            rightBound = new Vector2(hit.point.x - enemyWidthOffset, rightBound.y);
            //print("Collision on Right");
        }
        Debug.DrawLine(leftBound, rightBound, Color.green, 999f);
        //print(leftBound+  "," + rightBound);
    }

    void GenerateNearbyPlatformLinks()
    {
        if(platformData.Length > 0)
        {
            NearbyPlatformData plat;
            for(int i = 0; i < platformData.Length; i++)
            {
                plat = platformData[i];
                if (plat.overlapping)//Lower platforms
                {
                    switch (plat.side)
                    {
                        case NearbyPlatformData.sides.both:
                            plat.nearbyLeftPlatformLandingPoint = new Vector2(leftBound.x - horizontalDropDistance, plat.platform.GetComponent<Platform>().rightBound.y);
                            Debug.DrawLine(leftBound, plat.nearbyLeftPlatformLandingPoint, Color.red, 999);
                            plat.nearbyRightPlatformLandingPoint = new Vector2(rightBound.x + horizontalDropDistance, plat.platform.GetComponent<Platform>().leftBound.y);
                            Debug.DrawLine(rightBound, plat.nearbyRightPlatformLandingPoint, Color.red, 999);
                            break;
                        case NearbyPlatformData.sides.left:
                            plat.nearbyLeftPlatformLandingPoint = new Vector2(leftBound.x - horizontalDropDistance, plat.platform.GetComponent<Platform>().rightBound.y);
                            Debug.DrawLine(leftBound, plat.nearbyLeftPlatformLandingPoint, Color.red, 999);
                            break;
                        case NearbyPlatformData.sides.right:
                            plat.nearbyRightPlatformLandingPoint = new Vector2(rightBound.x + horizontalDropDistance, plat.platform.GetComponent<Platform>().leftBound.y);
                            Debug.DrawLine(rightBound, plat.nearbyRightPlatformLandingPoint, Color.red, 999);
                            break;
                        default:
                            print("Invalid platform side setting");
                            break;
                    }
                }
                else
                {
                    switch (plat.side)
                    {
                        case NearbyPlatformData.sides.left://Non-overlapping platforms physically can't be connected on both sides
                            Debug.DrawLine(leftBound, plat.platform.GetComponent<Platform>().rightBound, Color.red, 999);
                            break;
                        case NearbyPlatformData.sides.right:
                            Debug.DrawLine(rightBound, plat.platform.GetComponent<Platform>().leftBound, Color.red, 999);
                            break;
                        default:
                            print("Invalid platform side setting");
                            break;
                    }
                }
            }
        }
    }
}

















[Serializable]
public struct NearbyPlatformData//Assign these in the inspector
{
    public enum sides { both, right, left }
    public GameObject platform;
    public sides side;//0 if can be jumped to from either side of current platform, 1 if only on the left bound, 2 if only on the right bound
    public bool overlapping;//Whether or not the player jumps down to this platform or across to it (See pathfinding documentation in the discord for visuals)

    [HideInInspector] public Vector2 nearbyLeftPlatformLandingPoint;//These may or may not be used based on the platform. They are only used for overlapping platforms because non-overlapping ones just use the platform bounds
    [HideInInspector] public Vector2 nearbyRightPlatformLandingPoint;
}