using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyController : MonoBehaviour
{
    public float maxSpeed;

    private Rigidbody2D rb2d;
    private GameObject player;
    
    void Start() {
        rb2d = GetComponent<Rigidbody2D>();
        player = GameObject.FindWithTag("Player");
        
    }
    
    void Update() {
        // rotate gun
        //Vector3 mouseWorld = Camera.main.ScreenToWorldPoint(Input.mousePosition);
        //Vector2 mouseDelta = mouseWorld - gunPivot.position;
        //float angle = Mathf.Atan2(mouseDelta.y, mouseDelta.x) * 180 / Mathf.PI;
        //gunPivot.rotation = Quaternion.Euler(0, 0, angle);
        //
        //if(Input.GetButtonDown("Fire1")) {
        //    Instantiate(bullet, gun.position, gun.rotation);
        //}
    }

    void FixedUpdate() {
        Vector2 enemyDelta = player.transform.position - transform.position;
        Vector2 enemyInput = enemyDelta.normalized;
        rb2d.velocity = enemyInput * maxSpeed;
    }
}