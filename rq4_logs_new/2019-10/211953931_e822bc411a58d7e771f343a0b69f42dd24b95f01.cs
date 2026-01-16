using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Myles
{
    public class PuzzleGate : MonoBehaviour
    {
        private float time = 300;

        public GameObject gate;
        public GameObject openGate;

        void Start()
        {
            StartCoundownTimer();
        }

        void StartCoundownTimer()
        {
            if (time != null)
            {
                time = 300;
                
                InvokeRepeating("UpdateTimer", 0.0f, 0.01667f);
            }
        }

        void UpdateTimer()
        {
            if (time != null)
            {
                time -= Time.deltaTime;
                 
            }
        }

        private void OnMouseDown()
        {
            if (time > 0)
            {
                if (Inventory.main.HasItem(Item.GateKey))
                {
                    gate.SetActive(false);
                    openGate.SetActive(true);
                }
            }
            

            
        }
    }
}