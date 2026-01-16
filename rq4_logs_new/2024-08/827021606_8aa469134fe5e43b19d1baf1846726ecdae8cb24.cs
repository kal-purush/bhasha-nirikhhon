using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class AgreementTermsManager : MonoBehaviour
{
    public GameObject eulaPanel; // Reference to the UI panel containing the EULA
    public Button acceptButton; // Reference to the Accept button
    public Button declineButton; // Reference to the Decline button

    void Start()
    {
        ShowEULA();
    }

    private void ShowEULA()
    {
        eulaPanel.SetActive(true); // Show the EULA panel
        acceptButton.onClick.AddListener(OnAccept);
        declineButton.onClick.AddListener(OnDecline);
    }

    private void OnAccept()
    {
        eulaPanel.SetActive(false); // Hide the EULA panel
        SceneManager.LoadScene("Tutorial"); // Load the tutorial scene
    }

    private void OnDecline()
    {
        // Handle decline, for example by exiting the game
        Application.Quit(); // Exit the game
    }
}