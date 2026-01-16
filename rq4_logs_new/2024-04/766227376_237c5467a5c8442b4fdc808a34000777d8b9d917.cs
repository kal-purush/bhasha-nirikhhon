// DeathScreen.cs
using UnityEngine;
using UnityEngine.SceneManagement;

namespace TPS.UI
{
    public class DeathScreen : MonoBehaviour
    {
        private int lastLevelIndex;

        private void Start()
        {
            lastLevelIndex = PlayerPrefs.GetInt("LastLevelIndex", 1);
        }

        public void PlayGame()
        {
            Debug.Log(lastLevelIndex);
            if (lastLevelIndex == 3)
            {
                SceneManager.LoadScene(3);
                SceneManager.LoadScene(2, LoadSceneMode.Additive);
            }
            else
            {
                SceneManager.LoadScene(1);
                SceneManager.LoadScene(2, LoadSceneMode.Additive);
            }
        }

        public void QuitGame()
        {
            SceneManager.LoadScene(0);
        }
    }
}