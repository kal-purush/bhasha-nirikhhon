using UnityEngine;
using UnityEngine.SceneManagement;

public class LoadMap2 : MonoBehaviour
{

    // Tên của scene mà bạn muốn load (đặt trong Inspector hoặc hardcode)
    public string sceneToLoad = "ChangeMapCutScene";

    // Hàm được gọi khi một đối tượng khác chạm vào trigger
    private void OnTriggerEnter2D(Collider2D other)
    {
        // Kiểm tra xem đối tượng chạm vào có tag cụ thể không (ví dụ: "Player")
        if (other.CompareTag("Player"))
        {
            // Load scene được chỉ định
            SceneManager.LoadScene(sceneToLoad);
        }
    }
}
