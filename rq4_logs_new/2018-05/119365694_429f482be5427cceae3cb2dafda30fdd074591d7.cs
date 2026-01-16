using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace bananaDiver.buoyancyController
{
    public class BuoyancySliderScript : MonoBehaviour
    {

        public static float yPosition = 0;

        private GameObject slider;

        private int maxYPosition = 115;

        private float maxBuoyancy = 1;

        // Use this for initialization
        void Start()
        {
            slider = GameObject.Find("Slider");
        }

        // Update is called once per frame
        void Update()
        {
            slider.transform.localPosition = new Vector3(0, calculateNewYposition(yPosition), 0);
        }

        int calculateNewYposition(float buoyancy)
        {
            buoyancy = Mathf.Clamp(buoyancy, -maxBuoyancy, maxBuoyancy);
            float yPosition = maxYPosition * buoyancy;

            return (int)yPosition;
        }
    }
}