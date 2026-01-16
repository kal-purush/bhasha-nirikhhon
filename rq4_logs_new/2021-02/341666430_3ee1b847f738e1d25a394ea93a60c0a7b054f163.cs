using System;
using UnityEngine;
using UnityEngine.UIElements;

public class PlayerMovement : MonoBehaviour
{
    public float jumpVelocity = 10f;
    public float speed = 20f;
    
    private Rigidbody2D _playerRb;
    private bool _grounded;
    private LayerMask _groundLayerMask;
    
    private void Start()
    {
        _playerRb = transform.GetComponent<Rigidbody2D>();
        _groundLayerMask = LayerMask.NameToLayer("Ground");
    }

    private void Update()
    {
        _playerRb.velocity = new Vector2(speed, _playerRb.velocity.y);
        if (_grounded && Input.GetButtonDown("Jump"))
        {
            _playerRb.velocity = Vector2.up * jumpVelocity;
        }
    }

    private void OnTriggerEnter2D(Collider2D other)
    {
        if (other.gameObject.layer == _groundLayerMask)
        {
            _grounded = true;
        }
    }
    
    private void OnTriggerExit2D(Collider2D other)
    {
        _grounded = false;
    }
}