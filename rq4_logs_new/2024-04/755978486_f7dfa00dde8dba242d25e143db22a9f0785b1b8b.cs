using UnityEngine;

public abstract class BaseProjectile : MonoBehaviour
{
    protected int Damage;
    protected LayerMask ActionLayer;
    protected LayerMask DestroyLayer;
    public bool IsEnemy;

    protected abstract void OnEnemyHitPlayerAction(GameObject player, Vector3 hitPosition);
    protected abstract void OnPlayerHitEnemyAction(GameObject enemy, Vector3 hitPosition);

    protected void OnTriggerEnter2D(Collider2D collider)
    {
        if (DestroyLayer == (DestroyLayer | (1 << collider.transform.gameObject.layer)))
        {
            Destroy(gameObject);
        }
        if (ActionLayer == (ActionLayer | (1 << collider.transform.gameObject.layer)))
        {
            bool isHitPlayer = collider.gameObject == PlayerManager.Instance.Player;
            if (IsEnemy && isHitPlayer)
            {
                OnEnemyHitPlayerAction(collider.gameObject, collider.transform.position);
                Destroy(gameObject);
            }
            else if (!IsEnemy && !isHitPlayer)
            {
                OnPlayerHitEnemyAction(collider.gameObject, collider.transform.position);
                Destroy(gameObject);
            }
        }
    }
}