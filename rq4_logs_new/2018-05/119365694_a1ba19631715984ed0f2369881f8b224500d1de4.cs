using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace bananaDiver.scoreController
{
    public class ScoreController : MonoBehaviour
    {

        public Sprite fullStar;
        public Sprite emptyStar;
        public GameObject starPrefab;

        public static bool levelCompleted = false;

        public static int score;
        private int maxScore = 3;
        private bool starsCompleted = false;

        // Use this for initialization
        void Start()
        {

        }

        // Update is called once per frame
        void Update()
        {
            if (levelCompleted && !starsCompleted)
                createStars();
        }

        void createStars()
        {
            // Create full stars
            for (int i = 0; i < maxScore; i++)
            {
                GameObject GO = Instantiate(starPrefab);
                Image image = GO.GetComponent<Image>();
                if (score - (i + 1) >= 0)
                    image.sprite = fullStar;
                else
                    image.sprite = emptyStar;
                GO.transform.SetParent(gameObject.transform);
                GO.transform.localPosition = new Vector3(i * 125, 0, 0);
            }
            starsCompleted = true;
        }
    }
}