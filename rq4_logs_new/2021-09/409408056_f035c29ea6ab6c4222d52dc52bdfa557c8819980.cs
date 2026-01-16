using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Destroyable : MonoBehaviour
{
    [SerializeField] protected float maxHealth;
    [SerializeField] protected float deathDuration;
    [SerializeField] protected float health;
    public bool isDestroyed = false;

    
    public virtual void Init()
    {
        isDestroyed = false;
        health = maxHealth;
    }

    public virtual void Damage(float damage) {
        health -= damage;
        DamageAnim(damage);
        if (health <= 0) {
            StartCoroutine(Die());      
        }
    }

    protected virtual void DamageAnim(float damage){}
    protected virtual void Death() {
        isDestroyed = true;
        
    }
    protected virtual void DeathAnim(){}

    IEnumerator Die() {
        DeathAnim();
        yield return new WaitForSeconds(deathDuration);
        Death();
    }
}