using UI;
using UnityEngine;
using UnityEngine.SceneManagement;

namespace Scenes.GameOver
{
    public class GameOverSceneController : MonoBehaviour
    {
        [SerializeField] private Fader _fader;
        [SerializeField] private float _waitTimer;

        private bool _sceneActive;
        private float _currentCountdownTime;

        #region Unity Functions

        private void Start()
        {
            _fader.OnFadeInComplete += HandleSceneFadeIn;
            _fader.OnFadeOutComplete += SwitchScene;

            _fader.StartFadeIn();
        }

        private void OnDestroy()
        {
            _fader.OnFadeInComplete -= HandleSceneFadeIn;
            _fader.OnFadeOutComplete -= SwitchScene;
        }

        private void Update()
        {
            if (!_sceneActive)
            {
                return;
            }

            _currentCountdownTime -= Time.deltaTime;

            if (_currentCountdownTime <= 0)
            {
                _fader.StartFadeOut();
                _sceneActive = false;
            }
        }

        #endregion

        #region Utility Functions

        private void HandleSceneFadeIn()
        {
            _currentCountdownTime = _waitTimer;
            _sceneActive = true;
        }

        private void SwitchScene() => SceneManager.LoadScene(0);

        #endregion
    }
}