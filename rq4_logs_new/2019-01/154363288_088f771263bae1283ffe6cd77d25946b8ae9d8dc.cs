using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Scr_Star : Scr_AstroBase
{
    [Header("Planet Properties")]
    [SerializeField] private float movementSpeed;
    [SerializeField] float maxClampDistance;
    [SerializeField] float minClampDistance;

    [Header("References")]
    [SerializeField] private GameObject playerShip;
    [SerializeField] private GameObject mapVisuals;

    private double gravityConstant;
    private Vector3 lastFrameRotationPivot = Vector3.zero;
    private Rigidbody2D planetRb;
    private Rigidbody2D playerShipRb;

    private void Start()
    {
        planetRb = GetComponent<Rigidbody2D>();

        switchGravity = true;
        playerShipRb = playerShip.GetComponent<Rigidbody2D>();
        gravityConstant = 6.674 * (10 ^ -11);
        mapVisuals.SetActive(true);
    }

    public override void FixedUpdate()
    {
        if (playerShip.GetComponent<Scr_PlayerShipMovement>().onGround == false && switchGravity)
        {
            Vector3 translocation = transform.position;
            translocation = transform.position - translocation;

            float clampedDistance = Mathf.Clamp(Vector3.Distance(playerShip.transform.position, this.transform.position), minClampDistance, maxClampDistance);
            float clamp = Mathf.Lerp(1, 0, (clampedDistance - minClampDistance) / (maxClampDistance - minClampDistance));
            playerShip.transform.position += clamp * translocation;

            Vector3 gravityDirection = (transform.position - playerShip.transform.position);
            float gravity = (float)(planetRb.mass * playerShipRb.mass * gravityConstant) / ((gravityDirection.magnitude) * (gravityDirection.magnitude));
            playerShipRb.AddForce(gravityDirection.normalized * -gravity * Time.fixedDeltaTime);
        }
    }

    public override Vector3 GetFutureDisplacement(Vector3 position, float time)
    {
        transform.RotateAround(lastFrameRotationPivot, Vector3.forward, movementSpeed * (time - Time.fixedDeltaTime));
        Vector3 translocation = transform.position;
        transform.RotateAround(lastFrameRotationPivot, Vector3.forward, movementSpeed * Time.fixedDeltaTime);
        translocation = transform.position - translocation;

        float clampedDistance = Mathf.Clamp(Vector3.Distance(position, this.transform.position), minClampDistance, maxClampDistance);
        float clamp = Mathf.Lerp(1, 0, (clampedDistance - minClampDistance) / (maxClampDistance - minClampDistance));

        transform.RotateAround(lastFrameRotationPivot, Vector3.forward, -movementSpeed * time);

        return clamp * translocation;
    }

    public override Vector3 GetFutureGravity(Vector3 position, float time)
    {
        transform.RotateAround(lastFrameRotationPivot, Vector3.forward, movementSpeed * time);

        Vector3 gravityDirection = (transform.position - position);
        float gravity = (float)(planetRb.mass * playerShipRb.mass * gravityConstant) / ((gravityDirection.magnitude) * (gravityDirection.magnitude));
        transform.RotateAround(lastFrameRotationPivot, Vector3.forward, -movementSpeed * time);

        return gravityDirection.normalized * -gravity * Time.fixedDeltaTime;
    }

    private void OnDrawGizmos()
    {
        Gizmos.color = Color.yellow;
        Gizmos.DrawWireSphere(transform.position, minClampDistance);
        Gizmos.DrawWireSphere(transform.position, maxClampDistance);
    }
}