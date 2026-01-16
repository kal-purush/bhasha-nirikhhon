using UnityEngine;

public class CloseTutorialPopup : MonoBehaviour
{
    [Tooltip("閉じる対象のポップアップUIパネル")]
    public GameObject popupPanel;

    public void OnCloseButtonClicked()
    {
        if (popupPanel != null)
        {
            popupPanel.SetActive(false);
        }

        if (GameManager.Instance != null)
        {
            GameManager.Instance.StartGameAfterTutorial();
        }
        else
        {
            Debug.LogWarning("GameManager.Instance is null. ゲーム開始できません。");
        }
    }
}