using System.Collections;
using System.Collections.Generic;
using UnityEngine;
namespace Takens {
    public class BookNumber : MonoBehaviour
    {
        public int number;
        // Start is called before the first frame update
        void OnMouseDown()
        {

            GameObject.FindGameObjectWithTag("Canvas").GetComponent<InvGUI>().displayMessage(number.ToString());
            Inventory.main.currentCode[0] = Inventory.main.currentCode[1];
            Inventory.main.currentCode[1] = Inventory.main.currentCode[2];
            Inventory.main.currentCode[2] = Inventory.main.currentCode[3];
            Inventory.main.currentCode[3] = Inventory.main.currentCode[4];
            Inventory.main.currentCode[4] = Inventory.main.currentCode[5];
            Inventory.main.currentCode[5] = number;
        }
    }   
}