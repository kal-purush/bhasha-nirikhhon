using System;
using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

namespace Runtime.Misc
{
    public class TooltipMenu : MonoBehaviour
    {
        [SerializeField] private TextMeshProUGUI renderText;
        [SerializeField] private Vector3 textStartPos;
        [SerializeField] private Vector3 textEndPos;
        [SerializeField] private float moveDelay = .2f;
        [SerializeField] private float moveSpeed = .75f;
        [SerializeField] private bool allowDuplicates = false;
        
        private Queue<string> _queue = new();
        private bool _isAnimating;

        public void ShowText(string text)
        {
            bool isInQueue = _queue.Contains(text) && !allowDuplicates;
            bool isCurrentlyActive = renderText.text == text && !allowDuplicates;
            if (isInQueue || isCurrentlyActive) return;
            _queue.Enqueue(text);
        }

        private void Update()
        {
            if (_isAnimating) return;
            if (_queue.TryDequeue(out var result))
            {
                renderText.text = result;
                StartCoroutine(Animate());
            }
        }

        private IEnumerator Animate()
        {
            float progress = 0;
            _isAnimating = true;
            renderText.enabled = true;
            renderText.rectTransform.position = textStartPos;
            
            yield return new WaitForSeconds(moveDelay);
            
            while (progress < 1)
            {
                progress += Time.deltaTime * moveSpeed;
                progress = Mathf.Clamp01(progress);

                renderText.rectTransform.localPosition = Vector3.Lerp(textStartPos, textEndPos, progress);
                
                
                yield return null;
            }

            renderText.enabled = false;
            _isAnimating = false;
            renderText.text = "";
        }
        
    }
}