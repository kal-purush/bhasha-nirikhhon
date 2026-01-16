using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AnimalController : MonoBehaviour
{
    private Animator anim;

    public float moveSpeed = 2f;       // Tốc độ di chuyển
    public Vector3 areaCenter = Vector3.zero; // Tâm khu vực di chuyển (tùy chỉnh trong Inspector)
    public Vector3 areaSize = new Vector3(5f, 5f, 0f); // Kích thước khu vực di chuyển
    public float waitTime = 2f;       // Thời gian chờ trước khi chọn điểm mới

    private Vector3 targetPosition;   // Vị trí điểm đến tiếp theo
    private float waitTimer;          // Bộ đếm thời gian chờ

    void Start()
    {
        anim = GetComponent<Animator>();
        SetNewTargetPosition(); // Chọn điểm đầu tiên
    }

    void Update()
    {
        MoveTowardsTarget();
    }

    void MoveTowardsTarget()
    {
        // Di chuyển về phía điểm đến
        transform.position = Vector3.MoveTowards(transform.position, targetPosition, moveSpeed * Time.deltaTime);

        // Lật nhân vật nếu cần
        FlipCharacter();

        // Kiểm tra nếu đã đến nơi
        if (Vector3.Distance(transform.position, targetPosition) < 0.1f)
        {
            // Chờ trước khi chọn điểm mới
            waitTimer += Time.deltaTime;
            anim.SetBool("isIdle", true);
            if (waitTimer >= waitTime)
            {
                SetNewTargetPosition();
                waitTimer = 0f;
            }
        }
        else
        {
            anim.SetBool("isIdle", false);
        }
    }

    void SetNewTargetPosition()
    {
        // Tạo vị trí ngẫu nhiên trong khu vực dựa trên tâm và kích thước
        float randomX = Random.Range(areaCenter.x - areaSize.x / 2, areaCenter.x + areaSize.x / 2);
        float randomY = Random.Range(areaCenter.y - areaSize.y / 2, areaCenter.y + areaSize.y / 2);
        targetPosition = new Vector3(randomX, randomY, transform.position.z);
    }

    void FlipCharacter()
    {
        // Tính hướng di chuyển
        Vector3 direction = targetPosition - transform.position;

        // Lật nhân vật theo trục X
        if (direction.x > 0) // Di chuyển sang phải
        {
            transform.localScale = new Vector3(1.2f, 1.2f, 0f);
        }
        else if (direction.x < 0) // Di chuyển sang trái
        {
            transform.localScale = new Vector3(-1.2f, 1.2f, 0f);
        }
    }

    // Debug để hiển thị khu vực di chuyển trong Scene
    void OnDrawGizmos()
    {
        Gizmos.color = Color.yellow;
        Gizmos.DrawWireCube(areaCenter, areaSize); // Khu vực di chuyển theo tâm và kích thước
    }
}

