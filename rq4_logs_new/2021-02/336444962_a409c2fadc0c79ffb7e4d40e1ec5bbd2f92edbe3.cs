using System.Collections;
using System.Collections.Generic;
using UnityEngine;

enum EnemyType {Base, Distance, Wall}
public class EnemyController : MonoBehaviour
{
    [Header("Attributes")]
    [SerializeField] int maxHealth;
    [SerializeField] int currentHealth;
    [SerializeField] int damage; //Private, might need to add a property if an external difficulty script needs to change it.
    [SerializeField] EnemyState startingState;
    [SerializeField] float attackDistance;

    [Header("AttackColliders")]
    [SerializeField] LayerMask playerMask;
    [SerializeField] LayerMask playerHurtBox;
    //WIP: Need to add different attack colliders. 

    #region Private Variables
    EnemyAI ai; 
    Animator animator;
    bool isDead;
    bool isAttacking;

    public float AttackDistance { get => attackDistance; }
    #endregion

    void Start() {
        ai = GetComponent<EnemyAI>();
        ai.State = startingState;
        animator = GetComponentInChildren<Animator>();
        currentHealth = maxHealth;
    }

    void FixedUpdate()
    {
        ai.PathfindingLogic();
    }

    // public void OnReceiveDamage() {
    //     Destroy(gameObject);
    // }

    public void Attack() 
    {
        Collider2D Hit = Physics2D.OverlapCircle(transform.position, 10f, playerHurtBox);
        IEnemyHurtBox player = Hit?.GetComponent<IEnemyHurtBox>();
        player?.OnReceiveDamage();        
    }
}