using UnityEngine;
using UnityEngine.UI;
using Utils;

namespace UI
{
    [RequireComponent(typeof(Text))]
    public class TextFader : MonoBehaviour
    {
        [Header("Fade Rate")]
        [SerializeField] private float _fadeInRate;
        [SerializeField] private float _fadeOutRate;

        public delegate void FadeInComplete();
        public delegate void FadeOutComplete();

        public Fader.FadeInComplete OnFadeInComplete;
        public Fader.FadeOutComplete OnFadeOutComplete;

        private Text _fadeText;
        private bool _activateFadeIn;
        private bool _activateFadeOut;
        private float _currentAlpha;

        private bool _initialized;

        #region Unity Functions

        private void Start()
        {
            Initialize();
        }

        private void Update()
        {
            if (_activateFadeIn)
                FadeIn();
            else if (_activateFadeOut)
                FadeOut();
        }

        #endregion

        #region Utility Functions

        private void Initialize()
        {
            if (_initialized)
            {
                return;
            }

            _initialized = true;
            _fadeText = GetComponent<Text>();
            _currentAlpha = ExtensionFunctions.Map(_fadeText.color.a, 0, 1, 0, 255);
        }

        public void StartFadeIn(bool forceResetAlpha = false)
        {
            if (!_initialized)
            {
                Initialize();
            }

            if (forceResetAlpha)
            {
                _currentAlpha = 255;
            }

            _activateFadeIn = true;
            _activateFadeOut = false;
        }

        private void FadeIn()
        {
            _currentAlpha -= _fadeInRate * Time.deltaTime;

            Color fadeImageColor = _fadeText.color;
            _fadeText.color =
                ExtensionFunctions.ConvertAndClampColor(fadeImageColor.r, fadeImageColor.g, fadeImageColor.b,
                    _currentAlpha);

            if (!(_currentAlpha <= 0))
                return;

            OnFadeInComplete?.Invoke();
            _activateFadeIn = false;
            _fadeText.gameObject.SetActive(false);
        }

        public void StartFadeOut(bool forceResetAlpha = false)
        {
            if (!_initialized)
            {
                Initialize();
            }

            if (forceResetAlpha)
            {
                _currentAlpha = 0;
            }

            _fadeText.gameObject.SetActive(true);

            _activateFadeOut = true;
            _activateFadeIn = false;
        }

        private void FadeOut()
        {
            _currentAlpha += _fadeOutRate * Time.deltaTime;

            Color fadeImageColor = _fadeText.color;
            _fadeText.color =
                ExtensionFunctions.ConvertAndClampColor(fadeImageColor.r, fadeImageColor.g, fadeImageColor.b,
                    _currentAlpha);

            if (!(_currentAlpha >= 255))
                return;

            OnFadeOutComplete?.Invoke();
            _activateFadeOut = false;
        }

        #endregion
    }
}