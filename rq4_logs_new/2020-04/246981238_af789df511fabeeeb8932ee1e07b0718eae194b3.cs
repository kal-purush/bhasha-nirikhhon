using UnityEngine;
using UnityEngine.SceneManagement;

public class GameManager : MonoBehaviour
{
    public GameObject gameOverUI;
    public GameObject winLevelUI;

    public void loseGame()
        {
            Time.timeScale=0f;
            gameOverUI.SetActive(true);    
        }
    public void winLevel()
        {
            Time.timeScale=0f;
            winLevelUI.SetActive(true);
	    }
    public void Restart()
        {
            SceneManager.LoadScene(SceneManager.GetActiveScene().name);
            Time.timeScale=1f;
        }
    public void loadNextLevel()
        {
            SceneManager.LoadScene(SceneManager.GetActiveScene().buildIndex + 1);
            Time.timeScale= 1f;
        }
}