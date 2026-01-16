using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Common 
{
    public class DamageVolume : MonoBehaviour
    {
        public float damageValue;

        private void OnTriggerEnter2D(Collider2D collision)
        {
            var collisionScript = collision.GetComponent<Player.Movement.PlayerCollision>();
            if (collisionScript)
            {
                var InvestigatedTargetHealthSetter = collisionScript.GetComponent<Common.HealthSetter>();
                DealDamage(ref InvestigatedTargetHealthSetter, damageValue);
            }
        }
        public void DealDamage(ref Common.HealthSetter healthSetter, float _damage)
        {
            if (healthSetter.bCanDealDamage)
            {
                healthSetter.ReduceHealth(_damage);
                healthSetter.AllowDamage();
            }
        }

    }
}