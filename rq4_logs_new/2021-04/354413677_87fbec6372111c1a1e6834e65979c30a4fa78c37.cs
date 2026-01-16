using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyCombat : MonoBehaviour
{
    public Transform attackPoint;
    [Range(0, 5f)] [SerializeField]  float attackRange = .5f;
    public int attackDamage = 20;
    public float attackRate = 2f;
    float nextAttackTime = 0f;
    public LayerMask enemyLayers;
    
    [SerializeField] private bool showGizmos = true;
    
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {

    }

    public void Attack()
    {
        if(Time.time >= nextAttackTime)
        {
            Collider2D[] hitEnemies = Physics2D.OverlapCircleAll(attackPoint.position, attackRange, enemyLayers);
            foreach(Collider2D enemy in hitEnemies)
            {
                enemy.GetComponent<PlayerCombat>().TakeDamage(attackDamage);
            }
            nextAttackTime = Time.time + 1f / attackRate;
        }
        
    }

    void OnDrawGizmosSelected()
    {
        if(!showGizmos)
            return;
        if(attackPoint == null)
            return;

        Gizmos.DrawWireSphere(attackPoint.position, attackRange);
    }
}