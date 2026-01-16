using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
using UnityEngine.UI;
using EZCameraShake;
using System;

public class Sanesss : MonoBehaviour
{
    private GameObject bossHealthBar;
    private HealthBar bossHealthBarScript;
    public UnityEngine.AI.NavMeshAgent agent;
    private FarmerWalk farmerWalkScript;
    [SerializeField] private GameObject[] Summonables;
    [SerializeField] private GameObject FSCDrop;
    private PlayerController player;
    private Transform playerTransform;
    private UltimateBarCharge ultimateBar;

    [SerializeField] private SpriteRenderer mouseSpriteRenderer;
    [SerializeField] private GameObject pfDamagePopup;
    [SerializeField] private TextMeshPro pfDamagePopupText;
    [SerializeField] private Transform pfDamagePopupCrit;
    [SerializeField] private TextMeshPro pfDamagePopupTextCrit;

    private GameObject bossHealthBorder;
    private GameObject bossHealthFill;

    private bool alreadyDamaged;
    [SerializeField] private int maxMouseHealth;
    private int mouseHealth;

    private float bossCanAttack;
    private int bossDamage;

    private float RollAttackDelay1;
    private int AttackIndex1;

    private float RollAttackDelay2;
    private int AttackIndex2;

    private float RollAttackDelay4;
    private bool RollAttack4Check;
    private int AttackIndex4;

    private float DevilishWhaleDelay;

    private void Start()
    {
        FindObjectOfType<AudioManager>().PlaySound("SanesssIntro");
        FindObjectOfType<AudioManager>().PlayJingle("Sanesss");
        PlayerPrefs.SetInt("SanesssStatus", 1);
        maxMouseHealth = 20000;
        mouseHealth = maxMouseHealth;
        agent = GetComponent<UnityEngine.AI.NavMeshAgent>();
        agent.updateRotation = false;
        agent.updateUpAxis = false;

        player = GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerController>();
        playerTransform = GameObject.FindGameObjectWithTag("Player").transform;
        ultimateBar = GameObject.FindGameObjectWithTag("Ultimate Bar").GetComponent<UltimateBarCharge>();

        bossDamage = 100;
        RollAttackDelay1 = 3f;
        RollAttackDelay2 = 10f;
        RollAttackDelay4 = 2f;
        RollAttack4Check = false;
        DevilishWhaleDelay = 40f;

        alreadyDamaged = false;

        bossHealthBar = GameObject.FindGameObjectWithTag("BossHealthBar");
        bossHealthBorder = GameObject.FindGameObjectWithTag("BossHealthBorder");
        bossHealthFill = GameObject.FindGameObjectWithTag("BossHealthFill");
        bossHealthBorder.GetComponent<Image>().color = new Color32(0, 0, 0, 200);
        bossHealthFill.GetComponent<Image>().color = new Color32(135, 0, 0, 200);
        
        bossHealthBarScript = bossHealthBar.GetComponent<HealthBar>();
        bossHealthBarScript.SetMaxHealth(mouseHealth);
        bossHealthBar.SetActive(true);

        StartCoroutine(RollAttack1());
        StartCoroutine(RollAttack2());
        StartCoroutine(DevilishWhaleCycle());
    }

    private void Update()
    {
        if(mouseHealth <= 0)
        { 
            PlayerPrefs.SetInt("Sanesss", 2);
            Instantiate(FSCDrop, transform.position, Quaternion.identity);
            bossHealthBar.SetActive(false);
            FindObjectOfType<AudioManager>().StopSound("Sanesss");
            Destroy(gameObject);  
        }
    }

    private void FixedUpdate()
    {
        agent.SetDestination(playerTransform.position);
    }

    private IEnumerator RollAttack1()
    {  
        yield return new WaitForSeconds(RollAttackDelay1*UnityEngine.Random.Range(0.75f, 1.25f));
        AttackIndex1 = UnityEngine.Random.Range(0,2);

        if (AttackIndex1 == 0) { Instantiate(Summonables[0], playerTransform.position, Quaternion.Euler(0f,0f, UnityEngine.Random.Range(0f, 360f))); }
        if (AttackIndex1 == 1) { Instantiate(Summonables[1], new Vector3(playerTransform.position.x + UnityEngine.Random.Range(-4f, 4f), playerTransform.position.y + UnityEngine.Random.Range(-4f,4f), playerTransform.position.z), Quaternion.identity); }
        StartCoroutine(RollAttack1());
        yield return null;
    }

    private IEnumerator RollAttack2()
    {
        yield return new WaitForSeconds(RollAttackDelay2*UnityEngine.Random.Range(0.75f, 1.25f));
        FindObjectOfType<AudioManager>().PlaySound("SanesssTrigger");
        AttackIndex2 = UnityEngine.Random.Range(0,10);
        
        if (AttackIndex2 == 0) { StartCoroutine(TomBlaster1()); }
        if (AttackIndex2 == 1) { StartCoroutine(TomBlaster2()); }
        if (AttackIndex2 == 2) { StartCoroutine(TomBlaster3()); }
        if (AttackIndex2 == 3) { StartCoroutine(TomBlaster4I()); StartCoroutine(TomBlaster4II()); }
        if (AttackIndex2 == 4) { StartCoroutine(BoneStop()); }
        if (AttackIndex2 == 5) { StartCoroutine(SanessHead1()); }
        if (AttackIndex2 == 6) { StartCoroutine(SanessHead2()); }
        if (AttackIndex2 == 7) { StartCoroutine(ObstacleHead()); }
        if (AttackIndex2 == 8) { StartCoroutine(BoneSpin()); }
        if (AttackIndex2 == 9) { Instantiate(Summonables[6], playerTransform.position, Quaternion.Euler(0f,0f, UnityEngine.Random.Range(0f, 360f))); }

        StartCoroutine(RollAttack2());
        yield return null;
    }

    private IEnumerator RollAttack4()
    { 
        RollAttack4Check = true;
        yield return new WaitForSeconds(RollAttackDelay4*UnityEngine.Random.Range(0.75f, 1.25f));
        AttackIndex4 = UnityEngine.Random.Range(0,4);
        
        if (AttackIndex4 == 0) { for (int i = 0; i < 3; i++) {Instantiate(Summonables[7], transform.position, Quaternion.Euler(0f,0f, UnityEngine.Random.Range(0f,360f)), this.transform); } }
        if (AttackIndex4 == 1) { for (int i = 0; i < 4; i++) {Instantiate(Summonables[8], new Vector3(playerTransform.position.x + UnityEngine.Random.Range(-4f, 4f), playerTransform.position.y + UnityEngine.Random.Range(-4f,4f)), Quaternion.identity); } }
        if (AttackIndex4 == 2) { for (int i = 0; i < 5; i++) {Instantiate(Summonables[9], new Vector3(playerTransform.position.x + UnityEngine.Random.Range(-6f, 6f), playerTransform.position.y + UnityEngine.Random.Range(-6f,6f)), Quaternion.identity); } }
        if (AttackIndex4 == 3) { for (int i = 0; i < 5; i++) {Instantiate(Summonables[10], transform.position, Quaternion.Euler(0f,0f, UnityEngine.Random.Range(0f,360f))); } }
        yield return null;
    }

    private IEnumerator DevilishWhaleCycle()
    {
        yield return new WaitForSeconds(DevilishWhaleDelay*UnityEngine.Random.Range(0.75f, 1.25f));
        player.StartCoroutine(player.SummonDevilishWhale());
        StartCoroutine(DevilishWhaleCycle());
        yield return null;
    }

    private IEnumerator BoneSpin()
    {
        for (float i = 0; i < 2f; i++)
        {
            Instantiate(Summonables[4], transform.position, Quaternion.Euler(0f,0f, 90f*i), this.transform);
        }
        for (float a = 0; a < 2f; a++)
        {
            Instantiate(Summonables[5], transform.position, Quaternion.Euler(0f,0f, 90f*a + 180f), this.transform);
        }
        yield return null;
    }

    private IEnumerator ObstacleHead()
    {
        for (float x = 0; x < 11; x++)
        {
            float RowOffset = 2f*(x%2);
            FindObjectOfType<AudioManager>().PlaySound("SanesssHead");
            for (float y = 0; y < 11; y++)
            {
                Instantiate(Summonables[3], new Vector3(playerTransform.position.x + 4*(x-5), playerTransform.position.y + 4*(y-5) + RowOffset, playerTransform.position.z), Quaternion.identity);
                yield return new WaitForSeconds(0.02f);
            }
        }
        yield return null;
    }

    private IEnumerator SanessHead1()
    {
        for (int a = 0; a < 4; a++)
        {
            for (int i = 0; i < 5; i++)
            {
                Instantiate(Summonables[1], new Vector3(playerTransform.position.x + UnityEngine.Random.Range(-4f, 4f), playerTransform.position.y + UnityEngine.Random.Range(-4f,4f), playerTransform.position.z), Quaternion.identity);
            }
            yield return new WaitForSeconds(0.75f);
        }
        
        yield return null;
    }

    private IEnumerator SanessHead2()
    {
        for (int i = 0; i < 20; i++)
        {
            Instantiate(Summonables[1], new Vector3(playerTransform.position.x + UnityEngine.Random.Range(-3f,3f), playerTransform.position.y + UnityEngine.Random.Range(-3f,3f), playerTransform.position.z), Quaternion.identity);
            yield return new WaitForSeconds(0.15f);
        }
        yield return null;
    }

    private IEnumerator BoneStop()
    {
        for (float i = 0; i < 4f; i++)
        {
            Instantiate(Summonables[2], playerTransform.position, Quaternion.Euler(0f,0f, 90f*i));
        }
        yield return null;
    }

    private IEnumerator TomBlaster1()
    {
        for (int a = 0; a < 5; a++)
        {
            float DegreeOffset = 22.5f*UnityEngine.Random.Range(0,4);
            for (float i = 0; i < 4f; i++)
            {
                Instantiate(Summonables[0], playerTransform.position, Quaternion.Euler(0f,0f, 90f*i + DegreeOffset));
            }
            yield return new WaitForSeconds(0.45f);
        }
        yield return null;
    }

    private IEnumerator TomBlaster2()
    {
        for (int a = 0; a < 5; a++)
        {
            float XOffset = 1.5f*UnityEngine.Random.Range(0,2);
            for (int i = 0; i < 7; i++)
            {
                Instantiate(Summonables[0], new Vector3(playerTransform.position.x + 3f*(i-2) + XOffset + 0.25f, playerTransform.position.y + 0.2f, playerTransform.position.z), Quaternion.Euler(0f,0f, -90f));
            }
            yield return new WaitForSeconds(0.45f);
        }
            yield return null;
    }

    private IEnumerator TomBlaster3()
    {
        float DownOrUp = -1f + 2f*UnityEngine.Random.Range(0,2);
        for (int i = 0; i < 11; i++)
        {
            Instantiate(Summonables[0], new Vector3(playerTransform.position.x + 2f*(i%2), playerTransform.position.y + 1f*(i-5)*DownOrUp, playerTransform.position.z), Quaternion.Euler(0f,0f, 180f*(i%2)));
            yield return new WaitForSeconds(0.2f);
        }
        yield return null;
    }

    private IEnumerator TomBlaster4I()
    {
        for (int i = 0; i < 12; i++)
        {
            Instantiate(Summonables[0], playerTransform.position, Quaternion.Euler(0f,0f, UnityEngine.Random.Range(0f, 360f)));
            yield return new WaitForSeconds(0.75f);
        }
        yield return null;
    }

    private IEnumerator TomBlaster4II()
    {
        for (int i = 0; i < 4; i++)
        {
            for (int a = 0; a < 3; a++)
            {
                Instantiate(Summonables[0], playerTransform.position, Quaternion.Euler(0f,0f, UnityEngine.Random.Range(0f, 360f)));
            }
            yield return new WaitForSeconds(3f);
        }
        yield return null;
    }

    private void OnTriggerEnter2D(Collider2D other) 
    {
        if (other.gameObject.tag == "Player") { bossCanAttack = 0.5f; }
        if (alreadyDamaged == false)
        {
            if (other.gameObject.tag == "6sidedDice1") { mouseTakeDamage(1); ChargeUlt(6); }
            if (other.gameObject.tag == "6sidedDice2") { mouseTakeDamage(2); ChargeUlt(5); } 
            if (other.gameObject.tag == "6sidedDice3") { mouseTakeDamage(3); ChargeUlt(4); }
            if (other.gameObject.tag == "6sidedDice4") { mouseTakeDamage(4); ChargeUlt(3); }
            if (other.gameObject.tag == "6sidedDice5") { mouseTakeDamage(5); ChargeUlt(2); }
            if (other.gameObject.tag == "6sidedDice6") { mouseTakeDamage(6); ChargeUlt(1); }
            if (other.gameObject.tag == "BroomAttack") { mouseTakeDamage(10); ChargeUlt(8); }        

            if (other.gameObject.tag == "FakeDice1") { mouseTakeDamage(1); }
            if (other.gameObject.tag == "FakeDice2") { mouseTakeDamage(2); } 
            if (other.gameObject.tag == "FakeDice3") { mouseTakeDamage(3); }
            if (other.gameObject.tag == "FakeDice4") { mouseTakeDamage(4); }
            if (other.gameObject.tag == "FakeDice5") { mouseTakeDamage(5); }
            if (other.gameObject.tag == "FakeDice6") { mouseTakeDamage(6); }   
            if (other.gameObject.tag == "ChargedDice2") { mouseTakeDamage(2); ChargeUlt(2); }   
            if (other.gameObject.tag == "ChargedDice4") { mouseTakeDamage(4); ChargeUlt(4);  }   
            if (other.gameObject.tag == "ChargedDice6") { mouseTakeDamage(6); ChargeUlt(6); }   
            if (other.gameObject.tag == "FakeDice8") { mouseTakeDamage(8); ChargeUlt(8); }     
            if (other.gameObject.tag == "FakeDice10") { mouseTakeDamage(10); ChargeUlt(10); }     
            if (other.gameObject.tag == "FakeDice12") { mouseTakeDamage(12); ChargeUlt(12); }      
            if (other.gameObject.tag == "FakeDice20") { mouseTakeDamage(20); }     

            if (other.gameObject.tag == "Star") { mouseTakeDamage(UnityEngine.Random.Range(1,4) + Mathf.RoundToInt(((player.maxHealth - player.playerHealth)/player.maxHealth)*5)); }  
            if (other.gameObject.tag == "100sidedDice") { mouseTakeDamage(UnityEngine.Random.Range(100,200)); }   
        }
    }

    private void OnTriggerStay2D(Collider2D other)
    {   
        if (other.gameObject.tag != "Player") return;

        if (bossCanAttack >= 0.7f && player.playerAttacked == false)
        {
            if (player.Invincible == false) { FindObjectOfType<AudioManager>().PlaySound("PlayerHurt"); }
            else { FindObjectOfType<AudioManager>().PlaySound("Iframe"); }
            player.UpdateHealth(-bossDamage);
            bossCanAttack = 0f;
            player.playerAttacked = true;
        }    
        bossCanAttack += Time.deltaTime; 

        if (bossCanAttack <= 0.3f) {player.spriteRenderer.material.color = new Color32(255, 150, 150, 255);}
        else { player.spriteRenderer.material.color = new Color32(255, 255, 255, 255); }
    }

    private void OnTriggerExit2D(Collider2D collider)
    {
        if (collider.gameObject.tag == "6sidedDice1") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "6sidedDice2") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); } 
        if (collider.gameObject.tag == "6sidedDice3") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "6sidedDice4") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "6sidedDice5") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "6sidedDice6") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }  

        if (collider.gameObject.tag == "FakeDice1") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "FakeDice2") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); } 
        if (collider.gameObject.tag == "FakeDice3") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "FakeDice4") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "FakeDice5") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "FakeDice6") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }  
        if (collider.gameObject.tag == "BroomAttack") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }  
        if (collider.gameObject.tag == "ChargedDice2") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "ChargedDice4") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "ChargedDice6") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "FakeDice8") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "FakeDice10") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }
        if (collider.gameObject.tag == "FakeDice12") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }  
        if (collider.gameObject.tag == "FakeDice20") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }  
        
        if (collider.gameObject.tag == "Star") { alreadyDamaged = false; }  
        if (collider.gameObject.tag == "100sidedDice") { alreadyDamaged = false; mouseSpriteRenderer.material.color = new Color32(255, 255, 255, 255); }  

        if (collider.gameObject.tag == "Player") { player.spriteRenderer.material.color = new Color32(255, 255, 255, 255); }  
    }

    private void mouseTakeDamage(int i)
    {
        i = (int) (i * player.strength);
        CreateDamagePopup(i);
        mouseHealth -= i;
        mouseSpriteRenderer.material.color = new Color32(255, 150, 150, 255); 
        alreadyDamaged = true;
        bossHealthBarScript.SetHealth(mouseHealth);
        StartCoroutine(FlashingHealthBar());
        if (RollAttack4Check == false && mouseHealth < maxMouseHealth/2) { StartCoroutine(RollAttack4()); } 
    }

    private void CreateDamagePopup(int damageAmount)
    {
        pfDamagePopupText.text = damageAmount.ToString();
        pfDamagePopupTextCrit.text = damageAmount.ToString();
        if (damageAmount/player.strength <= 6) { Instantiate(pfDamagePopup, transform.position, Quaternion.identity); }
        else { Instantiate(pfDamagePopupCrit, transform.position, Quaternion.identity); }
    }

    private void ChargeUlt(int ChargeAmount)
    {
        if (ultimateBar.havePizza == true) { ultimateBar.IncreaseUltimateCharge(ChargeAmount); }
    }

    private IEnumerator FlashingHealthBar()
    {
        bossHealthBarScript.HealthBarFlash(1);
        yield return new WaitForSeconds(0.3f);
        bossHealthBarScript.HealthBarFlash(0);
        yield return null;
    }
}