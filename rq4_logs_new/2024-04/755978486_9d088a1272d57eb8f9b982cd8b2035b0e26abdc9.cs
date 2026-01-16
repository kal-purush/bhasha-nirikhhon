using System.Collections;
using System.Collections.Generic;
using Assets.Scripts.Combat;
using UnityEditor.EditorTools;
using UnityEngine;

#nullable enable annotations

[RequireComponent(typeof(Sword), typeof(FieldOfView))]
public class SwordWielderController : AIController, InputController
{
    [Header("AI parameters")]
    [Tooltip("Won't dash if player is shorter than this distance")]
    public float dashingDistance;
    [Tooltip("Preferred distance from player")]
    public float preferredDistance;

    private Sword _sword;
    private FieldOfView _fov;

    private Vector2 _home;


    void Awake()
    {
        _fov = GetComponent<FieldOfView>();
        if (!TryGetComponent(out _sword))
        {
            Debug.LogError("SwordWielderController requires Sword component to work properly");
        }
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
        ATTACKING,
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
            State.ATTACKING => StartCoroutine(AttackingState()),
            _ => throw new System.Exception("Invalid state")
        };

        // Update previous state to check for state changes in the future
        _prev_state = _state;
    }

    private float _idleMovement;
    private
    IEnumerator IdleState()
    {
        while (true)
        {
            // Move randomly around home
            var distFromHome = Vector2.Distance(transform.position, _home);
            if (distFromHome <= 5f)
            {
                _idleMovement = Random.Range(0.5f, 1f);
                _idleMovement *= Random.value < 0.5 ? 1 : -1;
            }
            else
            {
                _idleMovement = Mathf.Clamp(_home.x - transform.position.x, -0.5f, 0.5f);
            }

            // Check if player is in sight, if so, switch to attacking state
            for (int i = 0; i < 5; i++)
            {
                var canSeePlayer = _fov.FieldOfViewCheck();
                if (canSeePlayer)
                {
                    yield return new WaitForFixedUpdate();
                    _state = State.NOTICED;
                    yield break;
                }
                yield return new WaitForSeconds(0.2f);
            }
        }
    }

    IEnumerator NoticedState()
    {
        yield return new WaitForSeconds(0.5f);
        _state = State.ATTACKING;
        yield break;
    }

    private bool _attackingDash;
    private float _atackingMovement;
    IEnumerator AttackingState()
    {
        var player = PlayerManager.Instance.Player;
        while (true)
        {
            var distance = Vector2.Distance(player.transform.position, transform.position);
            _attackingDash = distance > dashingDistance;
            _atackingMovement = player.transform.position.x - transform.position.x;
            _atackingMovement -= Mathf.Sign(_atackingMovement) * preferredDistance;
            _atackingMovement = Mathf.Clamp(_atackingMovement, -1, 1);
            yield return new WaitForFixedUpdate();
        }
    }


    public override float GetHorizontalMovement()
    {
        var player = PlayerManager.Instance.Player;
        if (player == null)
        {
            return base.GetHorizontalMovement();
        }


        return _state switch
        {
            State.IDLE => _idleMovement,
            State.NOTICED => 0,
            State.ATTACKING => _atackingMovement,
            _ => base.GetHorizontalMovement()
        };
    }

    public override bool IsJumpPressed()
    {
        return _state switch
        {
            State.NOTICED => true,
            _ => base.IsJumpPressed()
        };
    }

    public override bool IsDashPressed()
    {
        var player = PlayerManager.Instance.Player;
        if (player == null)
        {
            return base.IsDashPressed();
        }


        return _state switch
        {
            State.ATTACKING => _attackingDash,
            _ => base.IsDashPressed()
        };
    }

    public override Vector2? GetAttackDirection()
    {
        var player = PlayerManager.Instance.Player;
        if (player == null)
        {
            return base.GetAttackDirection();
        }


        return _state == State.ATTACKING ? player.transform.position : null;
    }
}
