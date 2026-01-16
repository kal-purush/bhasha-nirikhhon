using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Assets.Scripts.Combat;
using UnityEngine;

// This is a template file for AIController
// IDLE util found player -> SHOOT -> RELOAD (while jumping) -> IDLE

#nullable enable annotations
public class TemplateController : AIController, InputController
{
    [SerializeField] private LayerMask _layerMask;
    private Gun _gun;


    void Awake()
    {
        _gun = GetComponent<Gun>();
    }

    // TODO: proper abstraction later (sealed class + pattern match?)
    // detect transition via functional callback?
    // pass common item (as struct)?
    enum State
    {
        INITAL,
        IDLE,
        SHOOTING,
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
            State.SHOOTING => StartCoroutine(ShootingState()),
            State.RELOADING => StartCoroutine(ReloadingState()),
            _ => throw new System.Exception("Invalid state")
        };

        // Update previous state to check for state changes in the future
        _prev_state = _state;
    }

    IEnumerator IdleState()
    {
        while (true)
        {
            var player = PlayerManager.Instance.Player;
            if (player == null)
            {
                yield return new WaitForSeconds(0.5f);
                continue;
            }

            // TODO: optimize

            // does it see the player?
            var casts = Physics2D.LinecastAll(transform.position, player.transform.position, _layerMask);
            var cast = casts.FirstOrDefault(c => c.transform.gameObject != gameObject);
            Debug.DrawLine(transform.position, player.transform.position, Color.red, 0.5f);
            Debug.DrawLine(transform.position, cast.point, Color.green, 0.5f);

            if (cast.transform.CompareTag("Player"))
            {
                yield return new WaitForFixedUpdate();
                _state = State.SHOOTING;
                yield break;
            }
            yield return new WaitForSeconds(0.5f);
        }
    }

    IEnumerator ShootingState()
    {
        while (true)
        {
            if (_gun.CurrentAmmo == 0)
            {
                yield return new WaitForFixedUpdate();
                _state = State.RELOADING;
                yield break;
            }

            // TODO: optimize
            var casts = Physics2D.LinecastAll(transform.position, PlayerManager.Instance.Player.transform.position, _layerMask);
            var cast = casts.FirstOrDefault(c => c.transform.gameObject != gameObject);
            if (!cast.transform.CompareTag("Player"))
            {
                yield return new WaitForFixedUpdate();
                _state = State.IDLE;
                yield break;
            }

            yield return new WaitForSeconds(0.5f);
        }
    }

    IEnumerator ReloadingState()
    {
        while (_gun.CurrentAmmo == 0)
        {
            yield return new WaitForFixedUpdate();
        }
        yield return new WaitForFixedUpdate();
        _state = State.IDLE;
        yield break;
    }


    public override Vector2? GetAttackDirection()
    {
        return _state == State.SHOOTING ? PlayerManager.Instance.Player.transform.position : null;
    }

    public override bool IsReloadPressed()
    {
        return _state == State.RELOADING;
    }

    public override bool IsJumpPressed()
    {
        return _state == State.RELOADING;
    }
}
