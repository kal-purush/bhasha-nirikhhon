using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(Collider2D))]
public class EnemyActivationPoint : MonoBehaviour
{
    [SerializeField]
    BaseNPC[] RegisteredEnemiesInThisArea;
    [SerializeField]
    Collider2D activationCollider; 

    void Start()
    {
        activationCollider = this.GetComponent<Collider2D>();
    }
    void OnTriggerEnter2D(Collider2D _collision)
    {
        var collisionScript = _collision.GetComponent<Player.Movement.PlayerCollision>();
        if (collisionScript)
        {
            // player is in the area
            // should activate enemies

            for (int i0 = 0; i0 < RegisteredEnemiesInThisArea.Length; i0++)
            {
                // all registered enemies are initalized
                RegisteredEnemiesInThisArea[i0].GetComponent<BaseNPC>().Init();
            }
        }
    }
}