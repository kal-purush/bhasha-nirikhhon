///<summary>
/// Author: Emily
///
/// Functions for the final endscreen
///
///</summary>

using Millivolt.Level;
using Millivolt.Player.UI;
using TMPro;
using UnityEngine;

namespace Millivolt.UI
{
	public class LevelCompleteScreen : MonoBehaviour
	{
        private GameObject m_closeScreen;

        [SerializeField] private TextMeshProUGUI m_timerText;


        [Header("Splash Text Settings")]
        [SerializeField] private TextMeshProUGUI m_splashText;
        [SerializeField] private GameObject m_splashtextParent;
        [SerializeField] private string[] m_splashtextChoices;

        [SerializeField] private Vector2 m_splashtextRotationRange;

        public void ActivateScreen()
        {
            // When this screen is activated set the game to the finish state
            GameManager.Instance.gameState = GameState.FINISH;

            // I know this isn't best practice however. I'm just grabbing the reference for a one time thing
            m_closeScreen = FindObjectOfType<PlayerDeathUI>(true).gameObject;

            // Activate the menu through the UIMenuManager
            GetComponent<UIMenu>().ActivateMenu();

            // Calculate the time in minutes, seconds and milliseconds
            float timer = LevelManager.Instance.levelTimer;
            int seconds = (int)timer % 60;
            int milliseconds = (int)(timer % 1 * 100);
            int minutes = (int)timer / 60;

            // Format text into minutes and seconds 
            string timerText = string.Format("{0:00}:{1:00}:{2:00}", minutes, seconds, milliseconds);

            // Set the value of the timer text to whatever the value of the timer is in LevelManager
            m_timerText.text = timerText;

            // Set Splashtext on level complete screen to random text from the array
            int randIndex = Random.Range(0, m_splashtextChoices.Length);
            m_splashText.text = m_splashtextChoices[randIndex];
        }

        /// <summary>
        /// Call to Restart the level after a delay
        /// </summary>
        public void RestartLevel()
        {
            GameManager.Instance.SetTimeScale(1);
            // Call the CRT effect to player
            m_closeScreen.SetActive(true);
            // Invoke the scene load function after the animation has finished
            Invoke(nameof(CallRestart), 1f);
        }

        /// <summary>
        /// Call to exit to menu after a delay
        /// </summary>
        public void ExitToMenu()
        {
            GameManager.Instance.SetTimeScale(1);
            // Call the CRT effect to player
            m_closeScreen.SetActive(true);
            // Invoke the scene load function after the animation has finished
            Invoke(nameof(CallExitToMenu), 1f);
        }

        private void CallRestart()
        {
            GameManager.Instance.gameState = GameState.PLAYING;
            GameManager.Instance.RestartLevel();
        }

        private void CallExitToMenu()
        {
            GameManager.Instance.ExitToMenu();
        }
    }
}