using UnityEngine;

public class ItemSpawnerOnDeath : MonoBehaviour
{
    [SerializeField]
    private GameObject itemPrefab;

    [SerializeField]
    private int minQuantity = 1;

    [SerializeField]
    private int maxQuantity = 3;

    [SerializeField]
    private float spawnHeight = 5f; // Chiều cao mà item sẽ xuất hiện phía trên vị trí chết

    [SerializeField]
    private float spawnRadius = 2f; // Bán kính phân tán ngang

    [SerializeField]
    [Tooltip("Tốc độ rơi ban đầu ngẫu nhiên (âm để rơi xuống)")]
    private float minFallSpeed = -2f;

    [SerializeField]
    private float maxFallSpeed = -5f;

    private Damageable damageable;

    private void Awake()
    {
        damageable = GetComponent<Damageable>();
        if (damageable == null)
        {
            Debug.LogWarning("ItemSpawnerOnDeath requires a Damageable component on the same GameObject.");
            return;
        }

        damageable.damageableDeath.AddListener(OnDeathSpawnItems);
    }

    private void OnDeathSpawnItems()
    {
        if (itemPrefab == null)
        {
            Debug.LogWarning("Item prefab is not assigned in ItemSpawnerOnDeath.");
            return;
        }

        int quantityToSpawn = Random.Range(minQuantity, maxQuantity + 1);

        for (int i = 0; i < quantityToSpawn; i++)
        {
            SpawnItem();
        }
    }

    private void SpawnItem()
    {
        // Tính toán vị trí spawn: ngẫu nhiên trong bán kính ngang và ở độ cao cố định
        Vector2 randomOffset = Random.insideUnitCircle * spawnRadius;
        Vector3 spawnPosition = transform.position + new Vector3(randomOffset.x, spawnHeight, 0);

        // Spawn item
        GameObject spawnedItem = Instantiate(itemPrefab, spawnPosition, Quaternion.identity);

        // Lấy Rigidbody2D
        Rigidbody2D rb = spawnedItem.GetComponent<Rigidbody2D>();
        if (rb == null)
        {
            Debug.LogWarning("Item prefab must have a Rigidbody2D component.");
            return;
        }

        // Thêm vận tốc rơi ngẫu nhiên
        float fallSpeed = Random.Range(minFallSpeed, maxFallSpeed);
        rb.linearVelocity = new Vector2(Random.Range(-1f, 1f) * spawnRadius * 0.2f, fallSpeed); // Thêm chút chuyển động ngang nhẹ

        // Optional: Thêm hiệu ứng quay
        rb.AddTorque(Random.Range(-100f, 100f));

        // Optional: Gán thuộc tính cho item
        Item itemComponent = spawnedItem.GetComponent<Item>();
        if (itemComponent != null)
        {
            itemComponent.Quantity = 1;
        }
    }

    // Các phương thức điều chỉnh từ Inspector
    public void SetItemPrefab(GameObject newPrefab)
    {
        itemPrefab = newPrefab;
    }

    public void SetQuantityRange(int min, int max)
    {
        minQuantity = Mathf.Max(1, min);
        maxQuantity = Mathf.Max(min, max);
    }

    public void SetSpawnHeight(float height)
    {
        spawnHeight = Mathf.Max(0, height);
    }

    public void SetSpawnRadius(float radius)
    {
        spawnRadius = Mathf.Max(0, radius);
    }

    public void SetFallSpeedRange(float minSpeed, float maxSpeed)
    {
        minFallSpeed = Mathf.Min(0, minSpeed); // Đảm bảo tốc độ là âm
        maxFallSpeed = Mathf.Min(minFallSpeed, maxSpeed); // maxSpeed nhỏ hơn hoặc bằng minSpeed
    }
}