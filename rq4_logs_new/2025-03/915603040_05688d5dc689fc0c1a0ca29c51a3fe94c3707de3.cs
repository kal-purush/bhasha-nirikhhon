using UnityEngine;

// Class này dùng để spawn item khi một object chết
public class ItemSpawnerOnDeath : MonoBehaviour
{
    [SerializeField]
    private GameObject itemPrefab; // Prefab của item sẽ được spawn

    [SerializeField]
    private int minQuantity = 1; // Số lượng item tối thiểu sẽ spawn

    [SerializeField]
    private int maxQuantity = 3; // Số lượng item tối đa sẽ spawn

    [SerializeField]
    private float spawnRadius = 2f; // Bán kính spawn item xung quanh vị trí chết

    private Damageable damageable; // Tham chiếu đến Damageable để xử lý sự kiện chết

    private void Awake()
    {
        damageable = GetComponent<Damageable>();
        if (damageable == null)
        {
            Debug.LogWarning("ItemSpawnerOnDeath requires a Damageable component on the same GameObject.");
            return;
        }

        // Đăng ký sự kiện chết từ Damageable
        damageable.damageableDeath.AddListener(OnDeathSpawnItems);
    }

    private void OnDeathSpawnItems()
    {
        if (itemPrefab == null)
        {
            Debug.LogWarning("Item prefab is not assigned in ItemSpawnerOnDeath.");
            return;
        }

        // Random số lượng item sẽ spawn trong khoảng min-max
        int quantityToSpawn = Random.Range(minQuantity, maxQuantity + 1);

        for (int i = 0; i < quantityToSpawn; i++)
        {
            SpawnItem();
        }
    }

    private void SpawnItem()
    {
        // Tính toán vị trí spawn ngẫu nhiên trong bán kính
        Vector2 randomOffset = Random.insideUnitCircle * spawnRadius;
        Vector3 spawnPosition = transform.position + new Vector3(randomOffset.x, randomOffset.y, 0);

        // Spawn item từ prefab
        GameObject spawnedItem = Instantiate(itemPrefab, spawnPosition, Quaternion.identity);

        // Optional: Gán các thuộc tính ban đầu cho item nếu cần
        Item itemComponent = spawnedItem.GetComponent<Item>();
        if (itemComponent != null)
        {
            // Có thể gán thêm các giá trị như Quantity hoặc InventoryItem nếu cần
            itemComponent.Quantity = 1; // Ví dụ gán số lượng là 1
        }
    }

    // Để dễ dàng điều chỉnh từ Inspector
    public void SetItemPrefab(GameObject newPrefab)
    {
        itemPrefab = newPrefab;
    }

    public void SetQuantityRange(int min, int max)
    {
        minQuantity = Mathf.Max(1, min);
        maxQuantity = Mathf.Max(min, max);
    }

    public void SetSpawnRadius(float radius)
    {
        spawnRadius = Mathf.Max(0, radius);
    }
}