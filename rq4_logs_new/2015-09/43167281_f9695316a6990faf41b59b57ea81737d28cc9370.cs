using UnityEngine;
using System.Collections;

public class Health : MonoBehaviour {

    public int health = 10;
    
    public void Damage(int amount) {
        health -= amount;
        if(health <= 0) {
            Destroy(this.gameObject);
        }
    }
}