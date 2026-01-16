using System.Collections;
using System.Collections.Generic;
using UnityEngine;
namespace Takens {
    public class Code : MonoBehaviour
    {
        void OnMouseDown()
        {
            if (!Inventory.main.hasItem(ItemType.message))
            {
               if(Inventory.main.selectedItem == ItemType.codeBreaker)
                {
                    GameObject.FindGameObjectWithTag("Canvas").GetComponent<InvGUI>().displayMessage("You cracked the code and found a hidden message!");
                    Inventory.main.Set(ItemType.message, true);
                }
               else
                {
                        GameObject.FindGameObjectWithTag("Canvas").GetComponent<InvGUI>().displayMessage("The text in this book seems to be encrypted...");
                }
            }
    }
    }
}