using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace Myles
{
    public class InventoryGUI : MonoBehaviour
    {
        public Text textHint;
        
        // Start is called before the first frame update
        void Start()
        {

        }

        // Update is called once per frame
        void Update()
        {
            if (Input.GetMouseButtonDown(0))
            {
                Inventory.main.itemBeingUsed = Item.None;
            }

            if (Inventory.main.itemBeingUsed == Item.None)
            {
                textHint.text = "";

            }
            else
            {
                textHint.text = $"Use {Inventory.main.itemBeingUsed} on ...";
            }

        }
    }
}