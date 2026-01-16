using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Girigiri
{

    public class LimitLine : MonoBehaviour
    {
        [SerializeField]
        private Sprite normalSprite;
        [SerializeField]
        private Sprite overSprite;
        private SpriteRenderer SpriteRenderer;
        private bool prevIsOver = false;

        // Use this for initialization
        void Awake()
        {
            SpriteRenderer = GetComponent<SpriteRenderer>();
            if (SpriteRenderer == null)
            {
                Debug.Log("SpriteRenderer is null.");
                Destroy(SpriteRenderer);
            }
            ChangeLine(false);
        }

        public void ChangeLine(bool isOver)
        {
            if (SpriteRenderer == null || prevIsOver == isOver) return;
            SpriteRenderer.sprite = (isOver) ? overSprite : normalSprite;
            prevIsOver = isOver;
        }

        public void Hide()
        {
            if (SpriteRenderer == null) return;
            SpriteRenderer.enabled = false;
        }

    }
}