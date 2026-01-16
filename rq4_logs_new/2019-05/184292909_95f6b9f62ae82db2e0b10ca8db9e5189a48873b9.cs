using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(BoxCollider2D))]
public class EnemyBehaviour : MonoBehaviour
{
    [SerializeField] private float Speed = 1.0f;
    [SerializeField] private float RotationSpeed = 1.0f;
    [SerializeField] private float BulletRange = 0.0f;
    [SerializeField] private float CooldownTime = 0.0f;
    [SerializeField] private Transform Target = null;

    private Quaternion lastRotation = new Quaternion();
    private float nextBullet = 0.0f;

    void Awake()
    { 
        if (Target ==  null)
            Debug.LogError("Missing Target Transform from a GameObject. Fill iTarget with sun object.");
    }

    // Update is called once per frame
    void Update()
    {
        lastRotation = transform.rotation;
        // Movement
        transform.position = Vector3.MoveTowards(transform.position, Target.position, Speed * Time.deltaTime);

        // TODO: Rotate it Properly
        Vector3 vectorToTarget = Target.position - transform.position;
        float angle = Mathf.Atan2(vectorToTarget.y, vectorToTarget.x) * Mathf.Rad2Deg;
        Quaternion qt = Quaternion.AngleAxis(angle, Vector3.forward);
        transform.rotation = Quaternion.RotateTowards(transform.rotation, qt, RotationSpeed * Time.deltaTime);

        // It's on aim, on Range and Cooldown OK
        if (lastRotation == transform.rotation && Vector3.Distance(Target.position, transform.position) <= BulletRange && nextBullet <= Time.time)
        {
            // TODO: Create an instance of the projectile.

            nextBullet = Time.time + CooldownTime;
        }
    }

    /// <summary>
    /// Change an enemy target.
    /// </summary>
    /// <param name="collision">Object returned by physics.</param>
    void OnCollisionStay2D(Collision2D collision)
    {
        if (collision.gameObject.transform != Target)
            Target = collision.gameObject.transform;
    }
}