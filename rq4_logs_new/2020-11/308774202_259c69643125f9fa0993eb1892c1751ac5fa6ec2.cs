using System;
using System.Collections;
using UnityEngine;
using UnityEngine.InputSystem;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

namespace PersonalDevelopment
{
    [RequireComponent(typeof(Button))]
    public class StartGameButtonBehaviour : MonoBehaviour
    {
        private Button _startGameButton = null;
        private bool _isLoading = false;
        private PlayerInputManager _inputManager = null;

        private PlayerInputManager InputManager
        {
            get
            {
                if (_inputManager == null)
                {
                    _inputManager = PlayerInputManager.instance;
                }

                return _inputManager;
            }
        }

        private void Awake()
        {
            _startGameButton = GetComponent<Button>();
        }

        private void OnEnable()
        {
            _startGameButton.onClick.AddListener(() =>
            {
                if (InputManager.playerCount > 0 && !_isLoading)
                {
                    _isLoading = true;
                    StartCoroutine(LoadYourAsyncScene());
                }
            });
        }
        
        private IEnumerator LoadYourAsyncScene()
        {

            // The Application loads the Scene in the background at the same time as the current Scene.
            AsyncOperation asyncLoad = SceneManager.LoadSceneAsync(Scene.Map.ToString(), LoadSceneMode.Additive);

            // Wait until the last operation fully loads to return anything
            while (!asyncLoad.isDone)
            {
                yield return null;
            }

            var players = FindObjectsOfType<PlayerInput>();
            // Move the GameObject (you attach this in the Inspector) to the newly loaded Scene
            for (int i = 0; i < players.Length; i++)
            {
                
                SceneManager.MoveGameObjectToScene(players[i].gameObject, SceneManager.GetSceneByName(Scene.Map.ToString()));
            }
        }

        private void OnDisable()
        {
            _startGameButton.onClick.RemoveAllListeners();
        }
    }
}