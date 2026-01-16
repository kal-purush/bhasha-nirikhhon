///<summary>
/// Author: Halen
///
/// Calls the emotion change method on the PlayerEmotions.
///
///</summary>

using System.Collections;
using UnityEngine;

namespace Millivolt.Player
{
    public class EmotionChanger : MonoBehaviour
    {
        [SerializeField, Min(0)] float m_emotionChangeDuration;
        
        public void SetEmotion(string newEmotion)
        {
            StopAllCoroutines();

            GameManager.Player.Emotion.ChangeEmotion(newEmotion);
        }

        public void ChangeEmotion(string newEmotion)
        {
            // override any existing emotion changes
            StopAllCoroutines();
            
            StartCoroutine(ChangeOnDelay(GameManager.Player.Emotion.currentEmotion, m_emotionChangeDuration));
            GameManager.Player.Emotion.ChangeEmotion(newEmotion);
        }

        private IEnumerator ChangeOnDelay(string emotionName, float delay)
        {
            yield return new WaitForSeconds(delay);
            SetEmotion(emotionName);
        }
    }
}