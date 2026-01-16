using System.Collections;
using System.Collections.Generic;
using Assets.Scripts.Combat;
using Unity.VisualScripting;
using UnityEngine;

#nullable enable annotations

[RequireComponent(typeof(Sword), typeof(FieldOfView), typeof(Ground))]
public class SwordChargerController : AIController, InputController
{

    [SerializeField] private float _patrolRadius = 5f;
    [SerializeField] private float _idleSpeed = 0.5f;
    [SerializeField] private int _jumpCount = 3;
    [SerializeField] private bool _superCharge = false;
    [SerializeField] private float _slashRange = 2f;
    [SerializeField] private float _cooldownDuration = 2f;

    private Vector2 _home;
    private Ground _ground;
    private FieldOfView _fov;
    private Sword _sword;



    void Awake()
    {
        _fov = GetComponent<FieldOfView>();
        _ground = GetComponent<Ground>();
        _sword = GetComponent<Sword>();
    }

    void Start()
    {
        _home = transform.position;
    }

    enum State
    {
        INITAL,
        IDLE,
        NOTICED,
        CHARGING,
        COOLDOWN,
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
            State.NOTICED => StartCoroutine(NoticedState()),
            State.CHARGING => StartCoroutine(ChargingState()),
            State.COOLDOWN => StartCoroutine(CooldownState()),
            _ => throw new System.Exception("Invalid state")
        };

        // Update previous state to check for state changes in the future
        _prev_state = _state;
    }

    private float _idleMovement;
    private IEnumerator IdleState()
    {
        _idleMovement = _idleSpeed;
        float lastPatrolTime = Time.time;

        while (true)
        {
            // Patrol around home
            var distFromHome = Vector2.Distance(transform.position, _home);
            if (distFromHome >= _patrolRadius || Time.time - lastPatrolTime > 5f)
            {
                _idleMovement = _home.x > transform.position.x ? _idleSpeed : -_idleSpeed;
                lastPatrolTime = Time.time;
            }

            // Check for player
            var canSeePlayer = _fov.FieldOfViewCheck();
            if (canSeePlayer)
            {
                yield return new WaitForFixedUpdate();
                _state = State.NOTICED;
                yield break;
            }

            yield return new WaitForSeconds(0.5f);
        }
    }

    private Vector2 _noticePosition;
    private IEnumerator NoticedState()
    {
        _noticePosition = PlayerManager.Instance.Player.transform.position;
        Debug.DrawLine(transform.position, _noticePosition, Color.yellow, 3f);
        // Wait for jumps
        for (int i = 0; i < _jumpCount; i++)
        {
            while (!_ground.IsOnGround)
            {
                yield return new WaitForFixedUpdate();
            }
            while (_ground.IsOnGround)
            {
                yield return new WaitForFixedUpdate();
            }
        }
        yield return new WaitForFixedUpdate();
        _state = State.CHARGING;
        yield break;
    }

    private float _chargingMovement;
    private bool _chargingSlash;
    private IEnumerator ChargingState()
    {
        float startTime = Time.time;
        while (true)
        {
            var ds = _noticePosition - (Vector2)transform.position;
            _chargingMovement = ds.x;
            if (!_superCharge)
            {
                _chargingMovement = Mathf.Clamp(_chargingMovement, -1, 1);
            }

            _chargingSlash = ds.magnitude <= _slashRange;
            Debug.DrawLine(transform.position, _noticePosition, Color.yellow, 0.1f);

            if (_chargingSlash || Time.time - startTime > 5f)
            {
                yield return new WaitForFixedUpdate();
                _state = State.COOLDOWN;
                yield break;
            }
            yield return new WaitForFixedUpdate();
        }
    }

    private IEnumerator CooldownState()
    {
        yield return new WaitForSeconds(_cooldownDuration);
        _state = State.IDLE;
        yield break;
    }

    #region InputController

    public override float GetHorizontalMovement()
    {
        return _state switch
        {
            State.IDLE => _idleMovement,
            // Face towards last player position
            State.NOTICED => _noticePosition.x > transform.position.x ? 0.1f : -0.1f,
            State.CHARGING => _chargingMovement,
            _ => base.GetHorizontalMovement()
        };
    }

    public override bool IsJumpPressed()
    {
        return _state == State.NOTICED;
    }

    public override bool IsDashPressed()
    {
        return _state == State.CHARGING && !_chargingSlash;
    }

    public override Vector2? GetAttackDirection()
    {
        return _state switch
        {
            State.CHARGING => _chargingSlash ? _noticePosition : null,
            _ => null
        };
    }

    #endregion
}