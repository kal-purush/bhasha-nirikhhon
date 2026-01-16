using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Assets.Scripts.Combat;
using UnityEngine;

#nullable enable annotations

[RequireComponent(typeof(Gun))]
public class GunnerController : AIController, InputController
{
    [SerializeField] private float _patrolRadius = 5f;
    [Tooltip("Preferred distance from player")]
    public float preferredDistance = 5f;

    [Tooltip("Chance to jump per second (on average)")]
    public float _jumpChance = 0.2f;

    [Header("Movement speed")]
    [Tooltip("Movement speed while patrolling")]
    [SerializeField] private float _idleSpeed = 0.5f;
    [Tooltip("Movement speed while shooting")]
    [SerializeField] private float _attackingSpeed = 0.8f;
    [Tooltip("Movement speed while reloading")]
    [SerializeField] private float _reloadingSpeed = 0.6f;



    private Vector2 _home;
    private Gun _gun;
    private FieldOfView _fov;



    void Awake()
    {
        _gun = GetComponent<Gun>();
        _fov = GetComponent<FieldOfView>();
    }

    void Start()
    {
        _home = transform.position;
    }

    enum State
    {
        INITAL,
        IDLE,
        ATTACKING,
        RELOADING
    }
    private State _state = State.IDLE;
    private State _prev_state = State.INITAL;
    private Coroutine? _state_coroutine = null;

#nullable enable

    void FixedUpdate()
    {
        // if state unchanged, nothing to do
        if (_prev_state == _state)
        {
            return;
        }

        // if state CHANGED, stop current coroutine
        if (_state_coroutine != null)
        {
            Coroutine c = _state_coroutine;
            _state_coroutine = null;
            StopCoroutine(c);
            return;
        }

        // if state CHANGED, start new coroutine depending on current state
        _state_coroutine = _state switch
        {
            State.IDLE => StartCoroutine(IdleState()),
            State.ATTACKING => StartCoroutine(AttackingState()),
            State.RELOADING => StartCoroutine(ReloadingState()),
            _ => throw new Exception("Invalid state")
        };

        // Update previous state to check for state changes in the future
        _prev_state = _state;
    }

    private float _idleMovement;
    IEnumerator IdleState()
    {
        var player = PlayerManager.Instance.Player;
        _idleMovement = player.transform.position.x > transform.position.x ? _idleSpeed : -_idleSpeed;
        float lastMoveTime = Time.time;
        Vector2 lastPos = transform.position;

        while (true)
        {
            // Check for player
            var canSeePlayer = _fov.FieldOfViewCheck();
            if (canSeePlayer)
            {
                yield return new WaitForFixedUpdate();
                _state = State.ATTACKING;
                yield break;
            }

            // Track last move time
            if (Vector2.SqrMagnitude(lastPos - (Vector2)transform.position) >= 0.01f)
            {
                lastMoveTime = Time.time;
            }

            // Patrol around home
            var distFromHome = Vector2.Distance(transform.position, _home);
            if (distFromHome >= _patrolRadius || Time.time - lastMoveTime > 1f)
            {
                _idleMovement = _home.x > transform.position.x ? _idleSpeed : -_idleSpeed;
            }

            yield return new WaitForSeconds(0.5f);
        }
    }

    private float _atackingMovement;
    IEnumerator AttackingState()
    {
        var player = PlayerManager.Instance.Player;

        while (true)
        {
            if (_gun.CurrentAmmo == 0)
            {
                yield return new WaitForFixedUpdate();
                _state = State.RELOADING;
                yield break;
            }

            _atackingMovement = player.transform.position.x - transform.position.x;
            _atackingMovement -= Mathf.Sign(_atackingMovement) * preferredDistance;
            _atackingMovement = Mathf.Clamp(_atackingMovement, -_attackingSpeed, _attackingSpeed);

            yield return new WaitForFixedUpdate();
        }
    }

    IEnumerator ReloadingState()
    {
        while (true)
        {
            if (_gun.CurrentAmmo != 0)
            {
                yield return new WaitForFixedUpdate();
                _state = _fov.FieldOfViewCheck() ? State.ATTACKING : State.IDLE;
                yield break;
            }

            var player = PlayerManager.Instance.Player;
            _atackingMovement = player.transform.position.x - transform.position.x;
            _atackingMovement -= Mathf.Sign(_atackingMovement) * preferredDistance;
            _atackingMovement = Mathf.Clamp(_atackingMovement, -_reloadingSpeed, _reloadingSpeed);

            yield return new WaitForFixedUpdate();
        }
    }

    public override float GetHorizontalMovement()
    {
        var a = _state switch
        {
            State.IDLE => _idleMovement,
            State.ATTACKING or State.RELOADING => _atackingMovement,
            _ => base.GetHorizontalMovement()
        };

        Debug.Log(a);

        return a;
    }

    public override bool IsJumpPressed()
    {
        return _state != State.IDLE && UnityEngine.Random.value < _jumpChance * Time.smoothDeltaTime;
    }

    public override Vector2? GetAttackDirection()
    {
        return _state == State.ATTACKING ? PlayerManager.Instance.Player.transform.position : null;
    }
}
