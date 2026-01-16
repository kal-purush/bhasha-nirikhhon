using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnityEngine;

namespace Assets
{
    public class ColorCalculator : IColorCalculator
    {
        private float r;
        private float g;
        private float b;
        private bool redTowardsPositive = false;
        private bool greenTowardsPositive = false;
        private bool blueTowardsPositive = false;
        private bool isRedActive = true;
        private bool isGreenActive = true;
        private bool isBlueActive = true;
        private float resolution;

        public ColorCalculator(float r, float g, float b, bool isRedActive, bool isGreenActive, bool isBlueActive, float resolution)
        {
            this.r = r;
            this.g = g;
            this.b = b;
            this.isRedActive = isRedActive;
            this.isGreenActive = isGreenActive;
            this.isBlueActive = isBlueActive;
            this.resolution = resolution;
        }

        public Color CalculateNextColor()
        {
            if (isRedActive)
            {
                redTowardsPositive = CheckDirection(r, redTowardsPositive);
                r = redTowardsPositive ? r += resolution : r -= resolution;
            }
            if (isGreenActive)
            {
                greenTowardsPositive = CheckDirection(g, greenTowardsPositive);
                g = blueTowardsPositive ? g += resolution : g -= resolution;
            }
            if(isBlueActive)
            {
                blueTowardsPositive = CheckDirection(b, blueTowardsPositive);
                b = blueTowardsPositive ? b += resolution : b -= resolution;
            }
            return new Color(r, g, b);
        }

        public bool CheckDirection(float color, bool currentState)
        {
            if(color >= 1)
            {
                return false;
            }
            else if(color <= 0)
            {
                return true;
            }
            else
            {
                return currentState;
            }
        }
    }
}