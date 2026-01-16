using UnityEngine;
using UnityEngine.SceneManagement;

public class GameManager : MonoBehaviour
{
    public GameObject gameOverUI;
    public GameObject winLevelUI;

    public void loseGame()
        {
            gameOverUI.SetActive(true);    
        }
    public void winLevel()
        {
            winLevelUI.SetActive(true);
	    }
    public void Restart()
        {
            SceneManager.LoadScene(SceneManager.GetActiveScene().name);   
        }
    public void loadNextLevel()
        {
            SceneManager.LoadScene(SceneManager.GetActiveScene().buildIndex + 1);
            Time.timeScale= 1f;
        }
}