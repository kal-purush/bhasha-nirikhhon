using UnityEngine;

public class Skeleton_Run : StateMachineBehaviour
{
    public float speed = 2.5f;
    public float attackRange = 3f;
    public float attackCooldown = 20f; // Adjust this value to control the attack cooldown

    Transform player;
    Rigidbody2D rb;
    Boss boss;
    bool hasAttacked = false;
    float nextAttackTime; // Time when the boss can attack again
    AudioManager audioManager;

    // OnStateEnter is called when a transition starts and the state machine starts to evaluate this state
    override public void OnStateEnter(Animator animator, AnimatorStateInfo stateInfo, int layerIndex)
    {
        audioManager = GameObject.FindGameObjectWithTag("Audio").GetComponent<AudioManager>();
        player = GameObject.FindGameObjectWithTag("Player").transform;
        rb = animator.GetComponent<Rigidbody2D>();
        boss = animator.GetComponent<Boss>();
        hasAttacked = false; // Reset the attack flag when entering the state
        // nextAttackTime = Time.time; // Initialize nextAttackTime to current time
    }

    // OnStateUpdate is called on each Update frame between OnStateEnter and OnStateExit callbacks
    override public void OnStateUpdate(Animator animator, AnimatorStateInfo stateInfo, int layerIndex)
    {
        boss.LookAtPlayer();
        audioManager = GameObject.FindGameObjectWithTag("Audio").GetComponent<AudioManager>();

        Vector2 target = new Vector2(player.position.x, rb.position.y);
        Vector2 newPos = Vector2.MoveTowards(rb.position, target, speed * Time.fixedDeltaTime);
        rb.MovePosition(newPos);

        // Check if it's time for the boss to attack based on the cooldown
        if (!hasAttacked && Time.time >= nextAttackTime && Vector2.Distance(player.position, rb.position) <= attackRange)
        {
            Debug.Log("WE LITTY");
            animator.SetTrigger("Attack");
            audioManager.PlaySFX(audioManager.fantasyBossAttack);
            hasAttacked = true; // Set the flag to true to prevent continuous attacks
            nextAttackTime = Time.time + attackCooldown; // Set the next attack time

        }
        // Update the nextAttackTime if the boss hasn't attacked yet
        if (hasAttacked)
        {
            nextAttackTime = Time.time + attackCooldown; // Set the next attack time
        }
    }


    // OnStateExit is called when a transition ends and the state machine finishes evaluating this state
    override public void OnStateExit(Animator animator, AnimatorStateInfo stateInfo, int layerIndex)
    {
        //Reset the attack flag when exiting the state
        hasAttacked = false;
        animator.ResetTrigger("Attack");
    }
}