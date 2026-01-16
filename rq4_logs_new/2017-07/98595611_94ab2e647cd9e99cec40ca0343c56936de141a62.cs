using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace Pause
{

    public class Pause : MonoBehaviour
    {

        [SerializeField] private GameObject pauseMenu;
        bool paused = false;

        void Start()
        {
            pauseMenu.SetActive(false);
        }

        void Update()
        {
            if (Input.GetButtonUp("Pause"))
            {
                if (!pauseMenu.activeInHierarchy)
                {
                    pauseGame();
                }
                else if (pauseMenu.activeInHierarchy)
                {
                    continueGame();
                }
            }
        }
        private void pauseGame()
        {
            Time.timeScale = 0;
            paused = true;
            pauseMenu.SetActive(true);
        }
        private void continueGame()
        {
            Time.timeScale = 1;
            paused = false;
            pauseMenu.SetActive(false);
        }

        public bool getPaused ()
        {
            return paused;
        }
    }
}