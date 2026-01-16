using System;
using UnityEngine;

public class MovementController : MonoBehaviour
{
    [SerializeField] private float speed = 50;
    
    private Transform target;
    private Action targetReached;
    
    public void SetMovementTarget(Transform target, Action targetReached)
    {
        this.target = target;
        this.targetReached = targetReached;
    }
    
    void Update()
    {
        if (target != null)
        {
            var direction = (target.position - transform.position).normalized;
            var movement = Time.deltaTime * speed * direction;
            float distanceToTarget = Vector3.Distance(transform.position, target.position);
            var clampedMovement = Vector3.ClampMagnitude(movement, distanceToTarget);
            transform.position += clampedMovement;

            if (HasReachedTarget(target))
            {
                targetReached?.Invoke();
                target = null;
            }
        }
    }

    private bool HasReachedTarget(Transform target)
    {
        return Vector3.Distance(transform.position, target.position) < 0.05f; // TODO play with this threshold a bit
    }

    public void StopWalking()
    {
        target = null;
        targetReached = null;
    }
}