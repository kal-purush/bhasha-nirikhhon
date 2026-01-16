using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class playerStatus : MonoBehaviour
{
    public healthBar healthBar;
    public staminaBar staminaBar;

    public int maxHealth = 10;
    public int healthPoints;
    bool isDead = false;

    public float maxStamina = 5;
    public float currentStamina;
    public float totalStaminaSpend, sprintSpend;
    public bool isSprint = false;
    float staminaRecSpeed = 4f;
    bool staminaTick = false;

    public string whoCollide;
    public GameObject player;
    GameObject enemy;
    Animator anim;

    public KeyCode revive;

    void Start()
    {
        //player = GameObject.FindWithTag("Player");
        enemy = GameObject.FindWithTag("Enemy");
        anim = GetComponent<Animator>();
        healthPoints = maxHealth;
        healthBar.SetMaxHealth(maxHealth);
        staminaBar.SetMaxStamina(maxStamina);
        currentStamina = maxStamina;
    }


    void OnTriggerEnter (Collider col)
    {
        if(col.gameObject.tag == "enemyWeapon")
        {
            whoCollide = col.gameObject.tag;
            healthPoints -= col.gameObject.GetComponent<weaponCollider>().damage;
            print("remaining health: " + healthPoints);
            anim.SetTrigger("gotHit"); //ANIMATION
            healthBar.SetHealth(healthPoints);            
        }
    }

    
    void PlayerDead()
    {
        isDead = true;
        anim.SetBool("isDie", true);
        player.GetComponent<snsControl>().enabled = false;
        player.GetComponent<Collider>().enabled = false;
        //anim.enabled = false;
        print("PLAYER DEAD!!!!");

    }

    void PlayerRevive()
    {        
        print("REVIVE!");
        anim.SetTrigger("revive");
        anim.SetBool("isDie", false);
        player.GetComponent<snsControl>().enabled = true;
        player.GetComponent<Collider>().enabled = true;
        healthPoints = maxHealth;
        healthBar.SetMaxHealth(maxHealth);
        isDead = false;
    }

    IEnumerator StaminaRecovery()
    {
        yield return new WaitForSeconds(staminaRecSpeed);
        staminaTick = false;
        currentStamina += 1;
        staminaBar.SetStamina(currentStamina);
    }

    void StaminaOut()
    {
        isSprint = false;
    }

    void Update()
    {
        totalStaminaSpend = sprintSpend; //In case adding action with stamina, add variable here
        
        if (isSprint == true)
        {
            sprintSpend = 1;
            currentStamina -= totalStaminaSpend * Time.deltaTime;
            staminaBar.SetStamina(currentStamina);
        }
        if (isSprint == false)
        {
            sprintSpend = 0;
        }
        if (currentStamina != maxStamina && staminaTick == false)
        {
            staminaTick = true;
            StartCoroutine(StaminaRecovery());
        }

        if (isDead = true && Input.GetKey (revive))
        {
            PlayerRevive();
        }

        if (healthPoints <= 0 && isDead == false)
        {
            PlayerDead();
        }



    }
}