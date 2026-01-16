using UnityEngine;
using UnityEngine.SceneManagement;

public class MenuManager : MonoBehaviour
{
    public GameObject menu;
    public GameObject menuButton;
    public GameObject ARSession;

    public void OnMenuButtonClicked()
    {
        menu.SetActive(true);
        menuButton.SetActive(false);
        // Pause the application
        ARSession.GetComponent<ARManager>().PauseApplicaiton(true);
    }

    public void OnResetClicked()
    {
        SceneManager.LoadScene("Main");
    }

    public void OnUsageReportClicked()
    {
        Debug.Log("Usage Report Clicked");
    }

    public void OnCancelButtonClicked()
    {
        //SceneManager.LoadScene(mainSceneName);
        menu.SetActive(false);
        menuButton.SetActive(true);
        ARSession.GetComponent<ARManager>().PauseApplicaiton(false);
    }
}