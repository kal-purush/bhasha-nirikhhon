using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class Player : MonoBehaviour
{
    
    public float moveSpeed;
    public Rigidbody2D rb;
    private Vector2 moveDirection;

    public int maxHealth = 100;
    public int currentHealth = 100;
    public Healthbar healthbar;
    private int damage = 10;
    private int heal = 10;
    float elapsed = 0f;

    void Start()
    {
        currentHealth = maxHealth;
        healthbar.SetMaxHealth(maxHealth);
    }

    // Update is called once per frame
    void Update()
    {
        ProcessInputs();
        Die();
    }
    
    void FixedUpdate(){
        Move();
    }
    
    void ProcessInputs(){
        float moveX = Input.GetAxisRaw("Horizontal");
        float moveY = Input.GetAxisRaw("Vertical");
        
        moveDirection = new Vector2(moveX, moveY).normalized;

        if(Input.GetKeyDown(KeyCode.Space)){
            GiveHealth(heal);
        }
        // else if (Input.GetKeyDown(KeyCode.H))
        // {
        //     Die();
        // }
    }
    
    void Move(){
        rb.velocity = new Vector2(moveDirection.x * moveSpeed, moveDirection.y * moveSpeed);
    }

    void TakeDamage(int damage){
        if(currentHealth > 0){
            currentHealth -= damage;
            healthbar.SetHealth(currentHealth);
        }
    }

    void GiveHealth(int heal){
        if(currentHealth < 100){
            currentHealth += heal;
            healthbar.SetHealth(currentHealth);
        }
    }

    private void OnCollisionEnter2D(Collision2D other) {
        if(other.gameObject.tag.Equals("Enemy")){
            TakeDamage(damage);
        }
    }

    private void OnCollisionStay2D(Collision2D other) {
        elapsed += Time.deltaTime;
        if (elapsed >= 1f) {
            elapsed = elapsed % 1f;
            if(other.gameObject.tag.Equals("Enemy")){
                TakeDamage(damage);
            }
        }
          
    }

    void Die()
    {
        if(currentHealth <= 0){
            //Destroy(gameObject);
        }
        //SceneManager.LoadScene(0);
    }
}