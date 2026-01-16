using UI;
using UnityEngine;
using UnityEngine.UI;

namespace SaveSystem
{
    public class SaveManagerDisplay : MonoBehaviour
    {
        [SerializeField] private TextFader _saveFader;
        [SerializeField] private Text _saveFaderText;
        [SerializeField] private float _fadeOutWaitTime;

        private float _currentWaitTime;
        private bool _gameSaveCountDownActive;

        #region Unity Functions

        private void Start() => SaveManager.Instance.OnSaveComplete += HandleGameSaved;

        private void OnDestroy() => SaveManager.Instance.OnSaveComplete -= HandleGameSaved;

        private void Update()
        {
            if (!_gameSaveCountDownActive)
            {
                return;
            }

            _currentWaitTime -= Time.deltaTime;
            if (_currentWaitTime <= 0)
            {
                EndCountDown();
            }
        }

        #endregion

        #region Utility Functions

        private void HandleGameSaved()
        {
            if (_gameSaveCountDownActive)
            {
                return;
            }

            Debug.Log("Displaying Save Text");

            _currentWaitTime = _fadeOutWaitTime;
            _gameSaveCountDownActive = true;

            _saveFaderText.text = "Game Saved";
            _saveFader.StartFadeOut(true);
        }

        private void EndCountDown()
        {
            Debug.Log("Hiding Save Text");

            _gameSaveCountDownActive = false;
            _saveFader.StartFadeIn();
        }

        #endregion
    }
}