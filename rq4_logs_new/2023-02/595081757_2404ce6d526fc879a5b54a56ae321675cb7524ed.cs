using System;
using System.Collections;
using UnityEngine;

namespace Prefabs.VFX.Ritual
{
    public class RitualEffectController : MonoBehaviour
    {
        [SerializeField] private Material scrollingMat;
        [SerializeField] private float scrollAnimationDuration;

        private bool _startedLastLerp;

        private void OnEnable()
        {
            _startedLastLerp = false;
            StartCoroutine(StartAlphaLerp(.75f));
        }

        private void OnDisable()
        {
            scrollingMat.SetFloat("_Alpha", 0f);
        }

        private IEnumerator StartAlphaLerp(float targetAlpha)
        {
            var currentTime = 0f;
            var currentAlpha = scrollingMat.GetFloat("_Alpha");
            while (currentTime < scrollAnimationDuration)
            {
                currentTime += Time.deltaTime;
                var newAlpha = Mathf.Lerp(currentAlpha, targetAlpha, currentTime/scrollAnimationDuration);
                scrollingMat.SetFloat("_Alpha", newAlpha);
                yield return null;
            }

            if (!_startedLastLerp)
            {
                _startedLastLerp = true;
                StartCoroutine(StartAlphaLerp(.4f));
            }
        } 
    }
}