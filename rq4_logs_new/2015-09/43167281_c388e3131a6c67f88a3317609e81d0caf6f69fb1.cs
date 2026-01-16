using UnityEngine;
using System.Collections;

public class Projectile : MonoBehaviour {

    public float projectileSpeed = 10f;
    public int damage = 1;

    // Use this for initialization
    void Start() {
        GetComponent<Rigidbody2D>().velocity = transform.right * projectileSpeed;
    }

    void OnCollisionEnter2D(Collision2D other) {
        if (other.collider.tag == "Enemy") {
                Enemy enemy = other.gameObject.GetComponent<Enemy>();
                enemy.Damage(damage);
            }
            Destroy(this.GetComponent<GameObject>());
        }
    }