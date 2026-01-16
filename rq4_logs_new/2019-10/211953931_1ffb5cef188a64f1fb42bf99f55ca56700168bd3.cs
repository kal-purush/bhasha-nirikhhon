using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace Andrea
{
    public class InventoryController : MonoBehaviour
    {
        public Text textHint;

        void Start()
        {

        }

        // Update is called once per frame
        void Update()
        {
            if (Input.GetMouseButtonDown(0))
            {
                Inventory.singleton.itemSelected = Item.None;
            }

            if (Inventory.singleton.itemSelected == Item.None)
            {
                textHint.text = "";
            }
            else
            {
                textHint.text = $"Use { Inventory.singleton.itemSelected.ToString().ToLower() } on...";
            }
        }
    }
}