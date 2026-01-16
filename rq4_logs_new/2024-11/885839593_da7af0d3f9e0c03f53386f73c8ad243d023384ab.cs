using UnityEngine;
using UnityEngine.UI;

public class HintManager : MonoBehaviour
{
    public Text yellowText; // Tham chiếu đến Text "Yellow"
    public Text blueText;   // Tham chiếu đến Text "Blue"
    public Text redText;    // Tham chiếu đến Text "Red"

    private bool hasShownHint = false; // Biến để kiểm tra xem gợi ý đã hiển thị chưa

    void Start()
    {
        // Đảm bảo các chữ bị tắt khi trò chơi bắt đầu
        yellowText.gameObject.SetActive(false);
        blueText.gameObject.SetActive(false);
        redText.gameObject.SetActive(false);
    }

    void Update()
    {
        // Khi nhấn phím F và gợi ý chưa hiển thị
        if (Input.GetKeyDown(KeyCode.F) && !hasShownHint)
        {
            ShowHint(); // Hiển thị gợi ý
            hasShownHint = true; // Đảm bảo gợi ý chỉ hiển thị một lần
        }
    }

    void ShowHint()
    {
        // Hiển thị các chữ
        yellowText.gameObject.SetActive(true);
        blueText.gameObject.SetActive(true);
        redText.gameObject.SetActive(true);

        // Ẩn chữ sau 5 giây
        StartCoroutine(HideHintAfterDelay(1f));
    }

    System.Collections.IEnumerator HideHintAfterDelay(float delay)
    {
        yield return new WaitForSeconds(delay);

        // Tắt tất cả các chữ
        yellowText.gameObject.SetActive(false);
        blueText.gameObject.SetActive(false);
        redText.gameObject.SetActive(false);
    }
}