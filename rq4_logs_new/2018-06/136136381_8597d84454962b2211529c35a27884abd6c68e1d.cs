using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace Girigiri
{
    public class HighScore : MonoBehaviour
    {
        [SerializeField]
        private Text highScoreText;

        // Use this for initialization
        void Start()
        {
            if (highScoreText != null)
            {
                if (Score.HighScore > 0.0f)
                {
                    highScoreText.text = string.Format("最高売上 {0:C}", Score.HighScore);
                }
                else
                {
                    highScoreText.enabled = false;
                }
            }
        }
        public void Reset()
        {
            PlayerPrefs.DeleteKey(Score.PREFS_KEY);
            Start();
        }
    }
}