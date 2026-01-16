using System.Collections;
using System.Collections.Generic;
using UnityEngine;
namespace Takens {
    public class CandleLight : MonoBehaviour
    {
        public GameObject flame;
        public bool isOn = false;
        public bool isPuzzleCandle  = false;
        public bool isAssembled = true;
        
        private InvGUI i;
        public void Start()
        {
            i = GameObject.FindGameObjectWithTag("Canvas").GetComponent<InvGUI>();
        }

        // Update is called once per frame
        void Update()
        {

        }

        void OnMouseDown()
        {
            if (isPuzzleCandle && !isAssembled)
            {
                if(Inventory.main.selectedItem == ItemType.candleStick)
                {
                    this.gameObject.transform.GetChild(0).gameObject.SetActive(true);
                    isAssembled = true;
                    Inventory.main.Set(ItemType.candleStick, false);
                    Inventory.main.hasCompletedCandleStick = true;
                    i.displayMessage("*click*");
                }
                else if(Inventory.main.selectedItem != ItemType.empty)
                {
                    i.displayMessage("You can't use that here!");
                }
            }
            else if (!isOn) {
                if (Inventory.main.selectedItem == ItemType.tinderBox)
                {
                    StartCoroutine("LightFlame");
                    
                    
                }
                else if(Inventory.main.selectedItem != ItemType.empty)
                {
                    i.displayMessage("You can't use that here!");
                }
                else if (Inventory.main.hasItem(ItemType.tinderBox))
                {
                    i.displayMessage("Your not holding your matches!");
                }
                else
                {
                    i.displayMessage("You dont have any matches!");

                }
            }
        }

        IEnumerator LightFlame()
        {
            flame.SetActive(true);
            isOn = true;
            yield return new WaitForSeconds(10f);
            if (!Inventory.main.hasCompletedLightPuzzle)
            {
                isOn = false;
                flame.SetActive(false);
            }
        }
    }
}