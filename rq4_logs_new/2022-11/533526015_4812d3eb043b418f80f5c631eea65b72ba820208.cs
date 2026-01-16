using System;
using System.Collections.Generic;
using UnityEngine;

public class EnemyBehavior : MonoBehaviour
{
    public int health;
    public int damage;
    public float speed;
    public int targetingDistance;

    private int _health;
    private int _damage;

    private List<Vector2> _target = new List<Vector2>();

    private void Start()
    {
        _health = health;
        _damage = damage;
    }

    private void FixedUpdate()
    {
        //Rotation
        var subPos = Core.SubObject.transform.position;
        var subY = subPos.y;
        var subX = subPos.x;
        var pos = transform.position;
        var x = pos.x;
        var y = pos.y;
        transform.rotation = Quaternion.Euler(0, 0, Mathf.Atan2(subY - y, subX - x) * Mathf.Rad2Deg);

        //Movement
        _target.Insert(0, new Vector2(subX, subY));
        while (_target.Count > targetingDistance + 1) _target.RemoveAt(targetingDistance + 1);
        if (_target.Count > targetingDistance)
        {
            var target = _target[targetingDistance];
            var position = new Vector2(x, y);
            var newPos = position + (target - position).normalized * speed;
            transform.position = new Vector3(newPos.x, newPos.y, pos.z);
        }
    }

    private void OnCollisionEnter(Collision collision)
    {
        //Bullet detection here
        //Take damage
        //Delete bullet
        //idk sounds and particles and stuff @Mark
        if (false)
        {
            _health--;
            if (_health == 0) Destroy(this);
        }
        
        
        //Oh yea also player damage detection too I guess @Mark
    }
}