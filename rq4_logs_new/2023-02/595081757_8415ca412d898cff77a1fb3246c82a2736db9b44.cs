using System.Collections;
using System.Collections.Generic;
using Audio;
using UnityEngine;

namespace Puzzle
{
    public class NoFunctionPopupTrigger : MonoBehaviour
    {
        [SerializeField] private Transform canvasToShow;
        [SerializeField] private float animationDuration;

        [SerializeField] private List<SfxSO> soundsToPlay;

        public enum SoundMode
        {
            PlayOnce,
            Repeat
        }

        public SoundMode thisSoundMode;

        private Coroutine _lerper;

        private bool _isVisible;
        private bool _hasPlayedSound;
        private float _targetScale;

        public void HideShowPopup()
        {
            if (_isVisible)
            {
                _isVisible = false;
                _targetScale = 0f;
            }
            else
            {
                _isVisible = true;
                _targetScale = 1f;

                if (!_hasPlayedSound)
                {
                    foreach (SfxSO sfx in soundsToPlay)
                    {
                        SoundManager.Instance.PlaySfx(sfx);
                    }

                    if (thisSoundMode == SoundMode.PlayOnce)
                    {
                        _hasPlayedSound = true;
                    }
                }
            }

            if (_lerper != null)
            {
                StopCoroutine(_lerper);
            }

            _lerper = StartCoroutine(StartLerp(canvasToShow, _targetScale));
        }

        private IEnumerator StartLerp(Transform target, float targetScale)
        {
            var currentTime = 0f;
            var currentScale = target.localScale;
            while (currentTime < animationDuration)
            {
                currentTime += Time.deltaTime;
                var newScale = Vector3.Lerp(currentScale, new Vector3(targetScale, targetScale, targetScale),
                    currentTime / animationDuration);
                target.localScale = newScale;
                yield return null;
            }
        }
    }
}