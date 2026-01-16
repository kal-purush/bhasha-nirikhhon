using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(Rigidbody2D))]
public abstract class EntityBehaviour : MonoBehaviour {

    public BodyGenerator body = null;
    Rigidbody2D rigidBody;

    public static float MinimumLatchVelocitySqr = 1f;

    void Awake()
    {
        rigidBody = GetComponent<Rigidbody2D>();

        EntityAwake();
    }

    void Start()
    {
        EntityStart();
    }
    
    void Update()
    {
        EntityUpdate();
    }

    public virtual void EntityAwake()
    {

    }

    public virtual void EntityStart()
    {

    }

    public virtual void EntityUpdate()
    {

    }

    public virtual void EntityOnCollisionStay2D(Collision2D other) { }

    // When it enters another collider
    void OnCollisionStay2D(Collision2D other)
    {
        BodyGenerator body = other.gameObject.GetComponent<BodyGenerator>();

        // If what you collided with has a bodygenerator, that means it's a body and that you should latch on to it
        // But only latch on to it if you're velocity is small enough
        if (body != null && rigidBody.velocity.sqrMagnitude <= MinimumLatchVelocitySqr)
        {
            this.body = body;
            transform.parent = body.gameObject.transform;
            rigidBody.isKinematic = true;
        }

        EntityOnCollisionStay2D(other);
    }
}