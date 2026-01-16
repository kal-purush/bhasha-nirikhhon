using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Assets.Scripts.Combat;
using UnityEngine;

#nullable enable annotations

// Bomber
// Runs towards the player
// Explode on impact
// Has Low HP
public class BomberController : AIController, InputController
{
    [SerializeField] private LayerMask _layerMask;
    [SerializeField] private float _explosionTriggerRadius;
    [SerializeField] private float _explosionRadius;
    [SerializeField] private int _explosionDamage;

    void Awake()
    {
    }

    enum State
    {
        INITAL,
        IDLE,
        RUNNING
    }
    private State _state = State.IDLE;
    private State _prev_state = State.INITAL;
    private Coroutine? _state_coroutine = null;

#nullable enable
    void OnDestroy()
    {
        // Create Circle Sprite at the position of the bomber
        // TODO complete this
        GameObject explosion = new GameObject("Explosion");
        explosion.AddComponent<SpriteRenderer>().sprite = Resources.Load<Sprite>("Sprites/ExplosionPlaceholder");
        explosion.transform.position = transform.position;

        Explode();

        // Remove sprite after 0.5 seconds
        Destroy(explosion, 0.5f);


    }
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
            State.RUNNING => StartCoroutine(RunningState()),
            _ => throw new System.Exception("Invalid state")
        };

        // Update previous state to check for state changes in the future
        _prev_state = _state;
    }

    // Idle until player is in sight
    IEnumerator IdleState()
    {
        Debug.Log("Bomber Idling");
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

            // if see player, change state to RUNNING
            if (cast.transform.CompareTag("Player"))
            {
                yield return new WaitForFixedUpdate();
                _state = State.RUNNING;
                yield break;
            }
            yield return new WaitForSeconds(0.5f);
        }
    }

    IEnumerator RunningState()
    {
        Debug.Log("Bomber Running");
        var player = PlayerManager.Instance.Player;
        if (player == null)
        {
            yield break;
        }

        while (true)
        {
            // if collide with player, explode
            if (Vector2.Distance(transform.position, player.transform.position) < _explosionTriggerRadius)
            {
                Explode();
            }

            yield return new WaitForFixedUpdate();
        }
    }

    // Explode on impact
    private void Explode()
    {
        // Draw circle of explosion
        Debug.DrawLine(transform.position, transform.position + new Vector3(_explosionRadius, 0, 0), Color.red, 0.5f);


        Collider2D[] colliders = Physics2D.OverlapCircleAll(transform.position, _explosionRadius);

        // Damage every object in the explosion radius
        foreach (Collider2D hit in colliders)
        {
            Debug.Log(name + " exploded and hit " + hit.name);
            Vector2 knockbackDirection = (hit.transform.position - transform.position).normalized;

            // damage collided object
            if (hit.TryGetComponent(out Health health))
            {
                health.TakeDamage(_explosionDamage, knockbackDirection, 10);
            }

        }
        SoundManager.Instance.PlayExplosion();
        Destroy(gameObject);
    }

    // override aicontroller for movements and jumps
    public override float GetHorizontalMovement()
    {
        // check if player is to the right with transform
        if (PlayerManager.Instance.Player.transform.position.x > transform.position.x)
        {
            return 1f;
        }
        else if (PlayerManager.Instance.Player.transform.position.x < transform.position.x)
        {
            return -1f;
        }
        return 0f;
    }

    public override float GetVerticalMovement()
    {
        // check if player is below with transform
        if (PlayerManager.Instance.Player.transform.position.y < transform.position.y)
        {
            return -1f;
        }
        return 0f;

    }

    public override bool IsJumpPressed()
    {
        if (PlayerManager.Instance.Player.transform.position.y > transform.position.y)
        {
            return true;
        }

        return false;
    }

}
