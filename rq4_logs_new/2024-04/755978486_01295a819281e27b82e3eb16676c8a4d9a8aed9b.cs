using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Assets.Scripts.Combat;
using UnityEngine;

#nullable enable annotations


public class MenuBomberController : AIController, InputController
{
    [SerializeField] private LayerMask _layerMask;
    [SerializeField] private float _explosionTriggerRadius;
    [SerializeField] private float _explosionRadius;
    [SerializeField] private int _explosionDamage;

    void Start()
    {
        StartCoroutine(CountdownCoroutine());
    }

    enum State
    {
        INITAL,
        IDLE
    }
    private State _state = State.IDLE;
    private State _prev_state = State.INITAL;
    private Coroutine? _state_coroutine = null;

#nullable enable
    void OnDestroy()
    {
        Debug.Log("boom");
        // Create Circle Sprite at the position of the bomber
        // TODO complete this
        //GameObject explosion = new GameObject("Explosion");
        //explosion.AddComponent<SpriteRenderer>().sprite = Resources.Load<Sprite>("Sprites/ExplosionPlaceholder");
        //explosion.transform.position = transform.position;

        //Explode();

        // Remove sprite after 0.5 seconds
        //Destroy(explosion, 0.5f);


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
        
            _ => throw new System.Exception("Invalid state")
        };

        // Update previous state to check for state changes in the future
        _prev_state = _state;
    }

    // Idle until player is in sight
    private float _idleMovement;

    IEnumerator CountdownCoroutine()
    {
        // Wait for a random duration between 5 and 10 seconds
        float countdownDuration = UnityEngine.Random.Range(5f, 10f);
        yield return new WaitForSeconds(countdownDuration);

        // Trigger explosion after the countdown
        Explode();
    }

    IEnumerator IdleState()
    {
       
        while (true)
        {

            _idleMovement = Random.Range(0.5f, 1f);
            _idleMovement *= Random.value < 0.5 ? 1 : -1;

     

            yield return new WaitForSeconds(0.5f);
        
           
           
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
        if(SoundManager.Instance) SoundManager.Instance.PlayExplosion();
        Destroy(gameObject);
    }

    // override aicontroller for movements and jumps
    public override float GetHorizontalMovement()
    {
        // check if player is to the right with transform
      
        return _idleMovement;
    }

  

    public override bool IsJumpPressed()
    {


        float randomValue =Random.value;


        if (randomValue <= 0.0025f)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

}
