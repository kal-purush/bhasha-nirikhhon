using System.Collections;
using UI;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

namespace Scenes.Loading
{
    public class LoadingSceneController : MonoBehaviour
    {
        [SerializeField] private Slider _loadingSlider;
        [SerializeField] private Fader _fader;

        private AsyncOperation _loadingOperation;
        private bool _exitFaderFired;

        #region Unity Function

        private void Start()
        {
            _fader.OnFadeInComplete += HandleFadeInComplete;
            _fader.OnFadeOutComplete += HandleFadeOutComplete;

            _fader.StartFadeIn();
        }

        #endregion

        #region Utility Functions

        private IEnumerator LoadNextSceneAsync()
        {
            _loadingSlider.value = 0;

            int sceneIndex = LoadingSceneData.NextSceneIndex;
            _loadingOperation = SceneManager.LoadSceneAsync(sceneIndex);
            _loadingOperation.allowSceneActivation = false;

            while (!_loadingOperation.isDone)
            {
                _loadingSlider.value = _loadingOperation.progress;

                if (_loadingOperation.progress >= 0.9f && !_exitFaderFired)
                {
                    _exitFaderFired = true;
                    _fader.StartFadeOut();
                }

                yield return null;
            }
        }

        private void HandleFadeInComplete()
        {
            _fader.OnFadeInComplete -= HandleFadeInComplete;
            StartCoroutine(LoadNextSceneAsync());
        }

        private void HandleFadeOutComplete()
        {
            _fader.OnFadeOutComplete -= HandleFadeOutComplete;
            _loadingOperation.allowSceneActivation = true;
        }

        #endregion
    }
}