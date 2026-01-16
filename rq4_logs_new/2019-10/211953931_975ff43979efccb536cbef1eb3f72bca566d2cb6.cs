using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Takens
{

    

    public class DoorOpen : MonoBehaviour
    {

        public int doorNumber = 0;

        void OnMouseDown()
        {
            Debug.Log("hi");
            if (doorNumber == 0 && Inventory.main.hasFirstKey)
            {
                Inventory.main.hasUnlockedDoorOne = true;
                Debug.Log("playing");
                GetComponent<Animator>().SetBool("isOpened", true);


                //no longer need these scripts, the door object can stay
                GetComponent<BoxCollider>().enabled = false;
                this.enabled = false;
            }
        }
    }

}