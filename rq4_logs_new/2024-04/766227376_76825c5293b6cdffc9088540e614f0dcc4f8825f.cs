using UnityEngine;
using UnityEngine.SceneManagement;

public class LevelManager : MonoBehaviour
{
    [SerializeField] private SceneObject sceneObject;

    public void LoadScene(string sceneName, LoadSceneMode loadSceneMode = LoadSceneMode.Single)
    {
        SceneManager.LoadScene(sceneName, loadSceneMode);
    }

    public void QuitGame()
    {
        Application.Quit();
    }

    public void LoadLevel()
    {
        LoadScene(sceneObject);
        LoadPlayerSystem();
    }

    public void LoadScene()
    {
        LoadScene(sceneObject);
    }

    private void LoadPlayerSystem()
    {
        SceneManager.LoadScene("PlayerSystem", LoadSceneMode.Additive);
    }
}