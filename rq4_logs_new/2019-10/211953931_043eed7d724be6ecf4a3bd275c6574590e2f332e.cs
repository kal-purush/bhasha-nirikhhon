using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace Jennings {

    public class CountdownTimer : MonoBehaviour {
        float currentTime = 30f;
        float startingTime = 30f;

        [SerializeField] Text countdownText;

        private void OnMouseDown()
        {
            currentTime = startingTime;
        }

        private void Update()
        {
            currentTime -= 1 * Time.deltaTime;
            countdownText.text = currentTime.ToString("0");

            if (currentTime <= 0)
            {
                currentTime = 0;
                countdownText.text = currentTime.ToString("You are too slow!!");
            }
        }
    }
}