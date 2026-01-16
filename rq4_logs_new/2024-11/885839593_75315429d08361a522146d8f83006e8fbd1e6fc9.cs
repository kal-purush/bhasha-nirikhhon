using UnityEngine;

public class FloatingItem : MonoBehaviour
{
    public float floatSpeed = 2f; // Tốc độ nổi
    public float floatHeight = 0.5f; // Độ cao nổi

    private Vector3 startPos;

    void Start()
    {
        startPos = transform.position; // Lưu vị trí ban đầu
    }

    void Update()
    {
        // Tính toán vị trí mới dựa trên sóng sin
        float newY = startPos.y + Mathf.Sin(Time.time * floatSpeed) * floatHeight;
        transform.position = new Vector3(transform.position.x, newY, transform.position.z);
    }
}