using UnityEngine;
using UnityEngine.UI;
using System.IO;

public class UIDeleteSave : MonoBehaviour
{
    [SerializeField]
    private Button deleteSaveButton;

    [SerializeField]
    private GameObject confirmationPanel;

    [SerializeField]
    private Button yesButton;

    [SerializeField]
    private Button noButton;

    private const string FILE_NAME_FORMAT = "Profile_{0}.txt";

    private void Awake()
    {
        if (deleteSaveButton != null)
        {
            deleteSaveButton.onClick.AddListener(ShowConfirmationDialog);
        }
        else
        {
            Debug.LogError("Delete Save Button chưa được gán!");
        }

        if (confirmationPanel != null)
        {
            confirmationPanel.SetActive(false);

            if (yesButton != null)
            {
                yesButton.onClick.AddListener(() => ConfirmDelete(true));
            }
            if (noButton != null)
            {
                noButton.onClick.AddListener(() => ConfirmDelete(false));
            }
        }
        else
        {
            Debug.LogError("Confirmation Panel chưa được gán!");
        }
    }
    private void ShowConfirmationDialog()
    {
        if (confirmationPanel != null)
        {
            confirmationPanel.SetActive(true);
        }
    }
    private void ConfirmDelete(bool confirm)
    {
        confirmationPanel.SetActive(false);

        if (confirm && DBController.Instance != null)
        {
            string fileName = string.Format(FILE_NAME_FORMAT, DBController.Instance.GetCurrentProfileIndex());
            string filePath = Path.Combine(Application.persistentDataPath, fileName);

            if (File.Exists(filePath))
            {
                File.Delete(filePath);
                Debug.Log($"Đã xóa file save: {fileName}");

                // Reset dữ liệu trong DBController
                DBController.Instance.ResetPlayerData();
                UpdateUIAfterDelete();
            }
            else
            {
                Debug.Log("Không tìm thấy file save để xóa.");
            }
        }
    }
    
    private void UpdateUIAfterDelete()
    {
        ProfileUIManager uiManager = FindObjectOfType<ProfileUIManager>();
        if (uiManager != null)
        {
            uiManager.UpdateProfileUI();
        }
    }

    private void OnDestroy()
    {
        if (deleteSaveButton != null)
        {
            deleteSaveButton.onClick.RemoveListener(ShowConfirmationDialog);
        }
        if (yesButton != null)
        {
            yesButton.onClick.RemoveAllListeners();
        }
        if (noButton != null)
        {
            noButton.onClick.RemoveAllListeners();
        }
    }
}