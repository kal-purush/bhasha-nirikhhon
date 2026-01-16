using UnityEngine;

public class ChasePlayerOnSight : MonoBehaviour
{
    [Header("Player Detection")]
    [SerializeField]
    private float detectionRange = 5f; // Khoảng cách phát hiện player
    [SerializeField]
    private float stopDistance = 1f;   // Khoảng cách dừng lại khi đến gần player

    [Header("Movement")]
    [SerializeField]
    private float moveSpeed = 2f;      // Tốc độ di chuyển của NPC

    [Header("Animation Names")]
    [SerializeField]
    private string idleAnimationName = "Idle"; // Tên animation khi đứng yên
    [SerializeField]
    private string runAnimationName = "Run";   // Tên animation khi chạy

    private GameObject player;
    private Animator animator;
    private SpriteRenderer spriteRenderer;
    private Rigidbody2D rb; // Nếu bạn muốn sử dụng Rigidbody2D để di chuyển

    private bool isChasing = false;

    private void Awake()
    {
        // Lấy các component cần thiết
        animator = GetComponent<Animator>();
        spriteRenderer = GetComponent<SpriteRenderer>();
        rb = GetComponent<Rigidbody2D>();
    }

    private void Start()
    {
        // Tìm player trong scene
        player = GameObject.FindGameObjectWithTag("Player");

        if (player == null)
        {
            Debug.LogWarning($"Không tìm thấy Player với tag 'Player' trong scene! ({gameObject.name})");
        }

        // Đặt trạng thái ban đầu là Idle
        PlayAnimation(idleAnimationName);
    }

    private void Update()
    {
        if (player == null) return;

        // Tính khoảng cách đến player
        float distanceToPlayer = Vector2.Distance(transform.position, player.transform.position);

        // Kiểm tra nếu player nằm trong tầm phát hiện
        if (distanceToPlayer <= detectionRange && distanceToPlayer > stopDistance)
        {
            // Bắt đầu đuổi theo player
            isChasing = true;
            PlayAnimation(runAnimationName);
            MoveTowardsPlayer();
        }
        else
        {
            // Dừng lại và chuyển về trạng thái Idle
            isChasing = false;
            PlayAnimation(idleAnimationName);
            StopMoving();
        }
    }

    private void MoveTowardsPlayer()
    {
        if (player == null) return;

        // Tính hướng di chuyển về phía player
        Vector2 direction = (player.transform.position - transform.position).normalized;
        Vector2 targetVelocity = direction * moveSpeed;

        // Di chuyển NPC
        if (rb != null)
        {
            rb.linearVelocity = targetVelocity;
        }
        else
        {
            transform.position = Vector2.MoveTowards(transform.position, player.transform.position, moveSpeed * Time.deltaTime);
        }

        // Xoay sprite theo hướng player
        if (spriteRenderer != null)
        {
            spriteRenderer.flipX = player.transform.position.x < transform.position.x;
        }
    }

    private void StopMoving()
    {
        // Dừng di chuyển
        if (rb != null)
        {
            rb.linearVelocity = Vector2.zero;
        }
    }

    private void PlayAnimation(string animationName)
    {
        if (animator != null && !string.IsNullOrEmpty(animationName))
        {
            if (animator.HasState(0, Animator.StringToHash(animationName)))
            {
                animator.Play(animationName);
            }
            else
            {
                Debug.LogError($"Trạng thái animation '{animationName}' không tồn tại trong Animator của {gameObject.name}!");
            }
        }
        else
        {
            Debug.LogWarning($"Animator hoặc tên animation ({animationName}) không hợp lệ cho {gameObject.name}!");
        }
    }
}