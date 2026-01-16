using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Breu
{
    public class BreuClockFace : MonoBehaviour
    {
        public GameObject ClockHand;
        // Start is called before the first frame update
        void Start()
        {
            if (BreuWorldControllerr.main.placedClock == false)
            {
                ClockHand.SetActive(false);
            }

        }
        
        public void OnMouseUp()
        {
            if (BreuInventory.main.usingClock == true)
            {
                ClockHand.SetActive(true);
                BreuInventory.main.usingClock = false;
                BreuInventory.main.hasClock = false;
                BreuWorldControllerr.main.placedClock = true;
            }
        }

    }
}