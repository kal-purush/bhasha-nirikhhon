using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CharacterControllerAnt : MonoBehaviour
{
    public CharacterController _antController;
    [SerializeField]
    Vector3 _move;
    [SerializeField]
    private float _speed = 10;
    [Space]
    [SerializeField]
    private float _rotationSpeed = 10;

    [SerializeField]
    private Animator _animator;
    private void Start()
    {
        _antController = GetComponent<CharacterController>();
    }
    void Update()
    {
        _move = InputManager.Instance.Move;
    }
    private void FixedUpdate()
    {
        Vector3 move = new Vector3(_move.x, 0, _move.y);
        move = Vector3.Lerp(move, move.normalized * _speed, Time.fixedDeltaTime);
        //move in the direct of 
        _antController.Move(move * _speed * Time.fixedDeltaTime);
        
        if (move != Vector3.zero)
        {
            Quaternion newRotation = Quaternion.LookRotation(move.normalized);
            transform.rotation = Quaternion.Slerp(transform.rotation, newRotation, Time.fixedDeltaTime * _rotationSpeed);
        }
        
        if (move != Vector3.zero)
        {
            _animator.SetBool("isWalking", true);
        }
        else
        {
            _animator.SetBool("isWalking", false);
        }
    }
}