using System;
using UnityEngine;
using System.Collections;

public class Bullet : MonoBehaviour {

    public enum BulletOwner
    {
        Player, Enemy
    }

    public BulletOwner bulletOwner { get; set; }
    public float speed = 1f;
    public Vector2 direction = new Vector2(1f, 0f);
    public SpriteRenderer bulletSpriteRenderer;

    public float directionAngle, directionDegrees, newRotationRichochet;

    public Vector3 newDirection;

    public float bulletXPosition, bulletYPosition, ricochetChancePercent;

    public bool canRichochet, useHitEffects;
    
    // Use this for initialization
    void Start()
    {
        // Cache the sprite renderer on start when bullets are initially created and pooled for better performance
        bulletSpriteRenderer = gameObject.GetComponent<SpriteRenderer>();
    }

    void OnEnable()
    {
        // When the bullet is enabled, ensure it faces the correct direction
        transform.eulerAngles = new Vector3(0.0f, 0.0f, directionAngle * Mathf.Rad2Deg);
    }

    public bool GetRicochetChance()
    {
        return UnityEngine.Random.Range(0f, 100f) < ricochetChancePercent;
    }

    /// <summary>
    /// This is a basic reflection algorithm (works the same way that Vector3.Reflect() works, but specifically for Vector2, as at this time Unity do not have a Vector2-specific implementation.
    /// </summary>
    /// <param name="vector"></param>
    /// <param name="normal"></param>
    /// <returns></returns>
    public static Vector2 Reflect(Vector2 vector, Vector2 normal)
    {
        return vector - 2 * Vector2.Dot(vector, normal) * normal;
    }

    void OnCollisionEnter2D(Collision2D coll)
    {
        // Note that you will need a rigidbody2d and Collider2D (not trigger) set up on your bullets to support ricochet behaviour.
        // The objects that you wish bullets to bounce off of, will in turn need their own Collider2D (any type, and not set to trigger).
        // If you wish to tell bullets to ignore collisions on various objects, use layers and edit your Physics2D collision matrix to tell the layer your bullets use to ignore collisions with other layers you don't want involved.

        if (canRichochet)
        {
            // Get the bullet heading/direction based on the collision point and the bullet's current position.
            var heading = coll.contacts[0].point - (Vector2)transform.position;

            // Get a new direction for the bullet by working out it's reflection vector by feeding in the current heading, and the collision point normal.
            newDirection = Vector3.Reflect(heading, coll.contacts[0].normal).normalized;

            // As our bullets are controlled by feeding them an angle to travel in, we need to calculate the new reflected direction angle from our new direction, and convert that to work for eulerangles, then assign that back to the bullet.
            directionAngle = Mathf.Atan2(newDirection.y, newDirection.x);
            transform.eulerAngles = new Vector3(0.0f, 0.0f, directionAngle * Mathf.Rad2Deg);

            // Set a new chance to ricochet up based on the initially set chance to ricochet value.
            canRichochet = GetRicochetChance();
        }
        else
        {
            // If the bullet cannot ricochet, then disable it to send it back to the bullet object pool.
            gameObject.SetActive(false);
        }

        if (useHitEffects)
        {
            // Fetch a hit effect spark from our object pool to use for the bullet impact effect if enabled.
            var spark = ObjectPoolManager.instance.GetUsableSparkParticle();

            if (spark != null)
            {
                spark.transform.position = transform.position;
                spark.SetActive(true);
            }
        }
    }
    
    // Update is called once per frame
    void Update()
    {
        directionDegrees = directionAngle*Mathf.Rad2Deg;

        if (gameObject != null)
        {
            // Account for bullet movement at any angle
            bulletXPosition += Mathf.Cos(directionAngle) * speed * Time.deltaTime;
            bulletYPosition += Mathf.Sin(directionAngle) * speed * Time.deltaTime;

            transform.position = new Vector2(bulletXPosition, bulletYPosition);

            // If the bullet is no longer visible by the main camera, then set it back to disabled, which means the bullet pooling system will then be able to re-use this bullet.
            if (!bulletSpriteRenderer.isVisible) gameObject.SetActive(false);
        }
    }
}