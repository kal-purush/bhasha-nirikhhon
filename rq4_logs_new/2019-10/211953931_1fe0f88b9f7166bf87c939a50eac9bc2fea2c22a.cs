using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace BreuWeek7
{

    [RequireComponent(typeof(changeRooms))]
    public class Door2 : MonoBehaviour
    {
        void OnMouseDown()
        {
            if (Inventory.main.hasKey == true)
            {
                print("the door is now unlocked");
                GetComponent<changeRooms>().GoToNextRoom();
            }
            else
            {
                print("Door is locked");
            }
        }
    }
}