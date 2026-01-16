using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using UnityEngine;
using UnityEngine.AI;
using UnityEngine.UI;
using static UnityEditor.Progress;

public class EnemyBehavior : MonoBehaviour
{
    //public GameManager manager;
    NavMeshAgent agent;
    public Transform player;
    public LayerMask groundLayer, playerLayer;
    Slider healthBar;
    Animator animator;

    Vector3 walkPoint;
    bool walkPointSet;
    float walkPointRange;
    [Header("Stats")]
    public float AttackCooldown = 0.7f;
    bool alreadyAttacked;

    public float sightRange, attackRange;
    bool playerInSightRange, playerInAttackRange;
    public float attackDamage = 20;
    public float tetherRange = 0.5f;

    public PlayerHealth playerHealth;
    public float maxHealth = 60f;
    float currentHealth;
    private float baseHP = 250f;

    [Header("Loot")]
    public int goldTarget = 50;
    public int goldTargetDeviation = 6;
    public int expTarget = 80;
    public int expTargetDeviation = 9;
    private float currHpPercent;

    public void Awake()
    {
        agent = GetComponent<NavMeshAgent>();
        //var playertmp = manager.player;
        //var playertmp = GameObject.Find("Player");
        //player = playertmp.transform;
        //playerHealth = playertmp.GetComponent<PlayerHealth>();
        animator = GetComponentInChildren<Animator>();
        healthBar = GetComponentInChildren<Slider>();
    }



    // Start is called before the first frame update
    void Start()
    {
        currentHealth = maxHealth;
        healthBar.value = CalculateHealth();
        currHpPercent = currentHealth / maxHealth;
    }

    private float CalculateHealth() => currentHealth / maxHealth;

    private void OnDrawGizmosSelected()
    {
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position, sightRange);
    }


    // Update is called once per frame
    void Update()
    {
        playerInSightRange = Physics.CheckSphere(transform.position, sightRange, playerLayer);
        playerInAttackRange = Physics.CheckSphere(transform.position, attackRange, playerLayer);
        currHpPercent = currentHealth / maxHealth;
        //IncreaseDamage();
        if (canAttackPlayer)
        {
            if (!playerInSightRange && !playerInAttackRange)
                Patroling();
            if (playerInSightRange && !playerInAttackRange)
                ChasePlayer();
            else if (playerInAttackRange && playerInSightRange)
                AttackPlayer();
        }
    }
    bool test = false;
    // private void IncreaseDamage()
    // {
    //     var sunlightRot = TimeController.instance.sunLightRotation;
        
    //     if (sunlightRot >= 180)
    //     {
    //         if (test)
    //         {
    //             baseHP = 60;
    //             test = false;
    //         }
    //         maxHealth = baseHP + (sunlightRot -180);

    //         currentHealth = maxHealth * currHpPercent;
            
    //     }
    //     else
    //     {
    //         if (!test)
    //         {
    //             test = true;
    //             baseHP = 60 + 180;
    //         }
    //         maxHealth = baseHP - (sunlightRot);
    //         currentHealth = maxHealth * currHpPercent;
    //     }
    // }

    private void Patroling()
    {
        animator.SetBool("IsInVisionRange", false);
        if (!walkPointSet)
            SearchWalkPoint();
        else
            agent.SetDestination(walkPoint);

        Vector3 distanceToWalkPoint = transform.position - walkPoint;

        if (distanceToWalkPoint.magnitude < 1f)
            walkPointSet = false;
    }
    private void ChasePlayer()
    {
        animator.SetBool("IsInVisionRange", true);
        animator.SetBool("IsInAttackRange", false);
        agent.SetDestination(player.position);
    }

    private void AttackPlayer()
    {
        animator.SetBool("IsInAttackRange", true);
        if((transform.position - player.position).magnitude < tetherRange) 
            agent.SetDestination(transform.position);
        else agent.SetDestination(player.position);
        transform.LookAt(player);
        if(!alreadyAttacked && canAttackPlayer)
        {
            //Need to put attacking logic here 
            animator.SetTrigger("AttackPlayer");
            var delay = animator.GetCurrentAnimatorStateInfo(0).length;
            alreadyAttacked = true;
            if(playerHealth.currentHealth > 0)
            {
                Invoke(nameof(HitPlayer), delay * 0.3f);
            }   
        }
    }
    bool canAttackPlayer = true;
    void HitPlayer()
    {
        playerHealth.TakeDamage(attackDamage);
        Invoke(nameof(ResetAttack), AttackCooldown);
    }

    bool amIdead = false;
    public bool TakeDamage(float damage, out MobDrop loot, float stunDuration = 0)
    {
        if (amIdead)
            goto itsJoever;
        currentHealth -= damage;
        healthBar.value = CalculateHealth();
        animator.SetTrigger("GotHit");
        PauseAgent();
        if (currentHealth <= 0)
        {
            amIdead = true;
            animator.SetBool("Died", true);
            Invoke(nameof(DestroyEnemy), 5f); //animator.GetCurrentAnimatorStateInfo(0).length * 2);
            loot = DropLoot();
            this.enabled = false;
            return true;
        }
        Invoke(nameof(ResumeAgent), animator.GetCurrentAnimatorStateInfo(0).length + stunDuration);
    itsJoever:
        loot = null;
        return false;
    }
    void ResumeAgent() 
    {
        canAttackPlayer = true;
        agent.isStopped = false; 
    }

    void PauseAgent()
    {
        canAttackPlayer = false;
        agent.isStopped = true;
        animator.SetBool("IsInVisionRange", false);
        animator.SetBool("IsInAttackRange", false);
    }
    private void DestroyEnemy()
    {
        Destroy(gameObject);
        //CreateNewEnemy(gameObject, 1f);
        //CreateNewEnemy(gameObject, 2f);
    }

    private MobDrop DropLoot()
    {
        var random = new System.Random();
        var drop = new MobDrop()
        {
            Gold = random.Next(goldTarget - goldTargetDeviation, goldTarget + goldTargetDeviation),
            Experience = random.Next(expTarget - expTargetDeviation, expTarget + expTargetDeviation),
        };
        return drop;
    }

    private void CreateNewEnemy(GameObject _gameObject, float offset)
    {
        var clone = Instantiate(_gameObject, new Vector3(_gameObject.transform.position.x + offset, _gameObject.transform.position.y, _gameObject.transform.position.z + offset), Quaternion.identity);
        var item = clone.GetComponent<EnemyBehavior>();
        clone.GetComponent<NavMeshAgent>().enabled = true;
        item.enabled = true;
        item.healthBar.enabled = true;
        item.maxHealth = item.maxHealth * 2;
        item.attackDamage *= 2;
        item.GetComponentInChildren<CanvasScaler>().enabled = true;
        item.GetComponentInChildren<Canvas>().enabled = true;
        item.GetComponentInChildren<GraphicRaycaster>().enabled = true;
        item.GetComponentInChildren<Slider>().enabled = true;
    }

    private void SearchWalkPoint()
    {
        float randomZ = Random.Range(-walkPointRange, walkPointRange);
        float randomX = Random.Range(-walkPointRange, walkPointRange);

        walkPoint = new Vector3(transform.position.x + randomX, transform.position.y, transform.position.z + randomZ);

        if (Physics.Raycast(walkPoint, -transform.up, 2f, groundLayer))
            walkPointSet = true;
    }
    private void ResetAttack() => alreadyAttacked = false;

}