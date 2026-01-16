using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnityEngine;

namespace Assets.Scripts.Ally
{
    public class AllyFight : MonoBehaviour
    {
        private double allyHealth = 200;
        private GameObject enemy;
        private EnemyHealth enemyHealth;
        private EnemyMovement enemyMovement;
        private Collider allyCollider;
        private void OnCollisionEnter(Collision collision)
        {
            if (collision.collider.gameObject.GetComponents<EnemyMovement>().Any())
            {
                enemy = collision.collider.gameObject;

                enemyMovement = enemy.gameObject.GetComponent<EnemyMovement>();
                enemyMovement.Fight(transform);

                enemyHealth = enemy.gameObject.GetComponent<EnemyHealth>();
                
                allyCollider = GetComponent<Collider>();
                allyCollider.enabled = false;
            }

        }

        private void Update()
        {
            if(enemy != null)
            {
                allyHealth -= 25;
                enemyHealth.DecreaseHealth();
                allyCollider.enabled = true;
                
            }

            if(allyHealth <= 0)
            {
                enemyMovement.StopFight();
                Destroy(gameObject);
            }
        }
    }
}