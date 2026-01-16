using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyController : MonoBehaviour
{
    [SerializeField] private float _moveSpeed = 5f;
    private Animator _animator;
    private Vector2 _direction;
    private Rigidbody2D _rigidbody;
    private float _timer;
    float x;
    float y;
    bool isTrue;

    private void Awake()
    {
        _animator = GetComponent<Animator>();
        _rigidbody = GetComponent<Rigidbody2D>();

        x = 1;
        y = 1;
        isTrue = true;
    }

    private void FixedUpdate()
    {
        Move();
    }

    private void Move()
    {
        _direction = Vector2.zero;

        // float _horizontalInput = Input.GetAxisRaw("Horizontal");
        // float _verticalInput = Input.GetAxisRaw("Vertical");
        // _direction += new Vector2(_horizontalInput, _verticalInput);
        _timer += Time.deltaTime;
        if (_timer > 2)
        {
            if (isTrue)
            {
                x *= -1;
            }
            else
            {
                y *= -1;
            }

            _timer = 0;

            isTrue = !isTrue;
        }

        _direction = new Vector2(x, y);

        _direction.Normalize();


        _rigidbody.MovePosition(_rigidbody.position + _direction * Time.deltaTime * _moveSpeed);

        _animator.SetFloat("Speed", _direction.magnitude);

        if (_direction != new Vector2(0, 0))
        {
            Vector2 Scaler = transform.localScale;
            if (_direction.x < 0 && Scaler.x > 0)
            {
                Scaler.x *= -1;
            }
            else if (_direction.x > 0 && Scaler.x < 0)
            {
                Scaler.x *= -1;
            }
            transform.localScale = Scaler;
        }
    }
}