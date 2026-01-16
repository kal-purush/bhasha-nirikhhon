using UnityEngine;
using UnityEngine.SceneManagement;

public class StartGameScreen : MonoBehaviour
{
    public void StartGame()
    {
        SceneManager.LoadScene("SampleScene", LoadSceneMode.Single);
    }
}