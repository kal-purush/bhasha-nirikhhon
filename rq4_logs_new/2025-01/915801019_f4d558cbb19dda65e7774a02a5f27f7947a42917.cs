using System.Collections;
using UnityEngine;

namespace Sample.Scripts.Views
{
    public class CardController : MonoBehaviour
    {
        [SerializeField]
        private float duration;

        [SerializeField]
        private Transform model;

        [SerializeField]
        private Vector3 openPosition;

        private Vector3 _initialPosition;
        private bool _isOpen = false;
        private bool _isProcessing = false;

        private void Start()
        {
            _initialPosition = model.position;
        }

        public void Toggle()
        {
            if (_isProcessing) return;
            StartCoroutine(ToggleDoorCoroutine());
        }

        private IEnumerator ToggleDoorCoroutine()
        {
            var elapsedTime = 0f;
            var startPosition = model.position;
            var targetPosition = _isOpen ? _initialPosition : openPosition;

            _isProcessing = true;
            while (elapsedTime < duration)
            {
                elapsedTime += Time.deltaTime;
                model.position = Vector3.Lerp(startPosition, targetPosition, EaseInQuart(elapsedTime / duration));
                yield return null;
            }
            _isOpen = !_isOpen;
            _isProcessing = false;
        }
        
        private static float EaseInQuart(float x)
        {
            return Mathf.Pow(x, 4);
        }
    }
}