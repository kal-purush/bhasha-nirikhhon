using LlamAcademy.Sensors;
using UnityEngine;
using UnityEngine.AI;

public class Virus : MonoBehaviour
{
    public PlayerSensor playerSensor;
    public float explosionDamage = 50f; // Set the damage amount for the explosion

    private NavMeshAgent agent;
    private Transform playerTransform;
    private Animator animator;

    [SerializeField]
    private Canvas canvas;
    [SerializeField]
    private Transform canvasPosition;
    [SerializeField]
    private Vector3 canvasOffset = new Vector3(0, 1, 0);

    private void Start()
    {
        agent = GetComponent<NavMeshAgent>();
        agent.updateRotation = false; // Disable automatic rotation
        animator = GetComponent<Animator>(); // Get the Animator component

        playerSensor.OnPlayerEnter += PlayerDetected;
        playerSensor.OnPlayerExit += PlayerLost;
    }

    private void PlayerDetected(Transform player)
    {
        playerTransform = player;
        animator.SetTrigger("Roll"); // Trigger the roll animation
    }

    private void PlayerLost(Vector3 lastKnownPosition)
    {
        playerTransform = null;
        agent.ResetPath(); // Stop the agent from moving
        animator.SetTrigger("Idle"); // Trigger the idle animation
    }

    private void Update()
    {
        canvas.transform.rotation = Quaternion.identity;

        // Update canvas position
        if (canvas != null && canvasPosition != null)
        {
            canvas.transform.position = canvasPosition.position + canvasOffset; // Apply offset
        }
        if (playerTransform != null)
        {
            agent.SetDestination(playerTransform.position);
            RotateTowards(playerTransform.position);
        }
    }

    private void RotateTowards(Vector3 targetPosition)
    {
        Vector3 direction = targetPosition - transform.position;
        direction.y = 0; // Ensure the rotation is only on the Y-axis

        // Determine the target rotation based on direction
        Quaternion targetRotation = Quaternion.LookRotation(direction);

        // Keep the rotation constrained to the Y-axis only
        targetRotation = Quaternion.Euler(0, targetRotation.eulerAngles.y, 0);

        transform.rotation = targetRotation;

        // Flip the sprite based on the direction
        if (direction.x < 0)
        {
            transform.rotation = Quaternion.Euler(0, 180, 0);
        }
        else
        {
            transform.rotation = Quaternion.Euler(0, 0, 0);
        }
    }

    private void OnCollisionEnter(Collision collision)
    {
        if (collision.gameObject.TryGetComponent<PlayerController>(out PlayerController player))
        {
            animator.SetTrigger("Explode"); // Trigger the explode animation
        }
    }

    // This method will be called at the end of the explode animation
    public void OnExplosionEnd()
    {
        Debug.Log("Explosion animation ended."); // Example: Print a message when the explosion animation ends

        // Inflict damage on the player
        if (playerTransform != null)
        {
            playerTransform.GetComponent<PlayerStats>().TakeDamage(explosionDamage);
        }

        Destroy(gameObject); // Destroy the Virus GameObject after the explosion animation ends
    }
}