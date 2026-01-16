using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Crawlid : Enemy
{
    [SerializeField] private float rayLength = 5f;
    [SerializeField] private Vector2 dir = new Vector2(1, -1); //-1 is left, 1 is right

    [SerializeField] private float spd = 5f;
    
    
    protected override void Behavior()
    {
        CheckGroundFront();
        _rb.velocity = new Vector2(dir.x * spd, _rb.velocity.y);
    }

    private void CheckGroundFront()
    {
        var position = transform.position;
        LayerMask mask = LayerMask.GetMask("Ground");

        RaycastHit2D ray = Physics2D.Raycast(position, dir, rayLength, mask);

        Debug.DrawRay(position, dir * rayLength, Color.blue);

        if (!ray.collider)
        {
            var localScale = transform.localScale;
            localScale = new Vector3(-localScale.x, localScale.y, localScale.z);
            transform.localScale = localScale;
            dir = new Vector2(-dir.x, dir.y);
        }
    }
}