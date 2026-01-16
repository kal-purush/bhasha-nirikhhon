using UnityEngine;

public class BossAreaTrigger : MonoBehaviour
{
    public BossHealBar bossHealthBar; // Tham chiếu đến BossHealBar trong Inspector
    public string bossTag = "GruzMother"; // Tag của boss

    private void OnTriggerEnter2D(Collider2D other)
    {
        if (other.CompareTag("Player")) // Kiểm tra nếu là người chơi
        {
            GameObject boss = GameObject.FindGameObjectWithTag(bossTag);
            if (boss != null)
            {
                bossHealthBar.gameObject.SetActive(true); // Bật UI
                bossHealthBar.Initialize(boss); // Khởi tạo thanh máu với boss
                Debug.Log("Player entered boss area, showing health bar.");
            }
            else
            {
                Debug.LogWarning("Boss with tag '" + bossTag + "' not found in the scene.");
            }
        }
    }

    private void OnTriggerExit2D(Collider2D other)
    {
        if (other.CompareTag("Player"))
        {
            bossHealthBar.gameObject.SetActive(false); // Tắt UI khi người chơi rời khu vực
            Debug.Log("Player left boss area, hiding health bar.");
        }
    }
}