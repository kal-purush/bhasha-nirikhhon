using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Breu
{
    public class BreuOil : MonoBehaviour
    {
        public GameObject Oil;
        // Start is called before the first frame update
        void Start()
        {

        }

        // Update is called once per frame
        void Update()
        {

        }

        void OnMouseUp()
        {
            if (BreuInventory.main.foundOil == false)
            {
                BreuInventory.main.foundOil = true;
                BreuInventory.main.hasOil = true;
                Destroy(Oil);
            }
        }
    }
}