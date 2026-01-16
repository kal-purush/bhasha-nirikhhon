using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Jennings {
    public class ChangeRooms : MonoBehaviour {
        public Camera cameraThisRoom;
        public Camera cameraNextRoom;
        //creates bool preventing me from going to next room
        public bool gotoNextRoomOnClick = false;

        void OnMouseDown()
        {
            //When mouse clicked on object it takes you to next room if gotoNextRoomOnClick is true.
            if (gotoNextRoomOnClick) GotoNextRoom();
        }

        public void GotoNextRoom()
        {
            cameraThisRoom.gameObject.SetActive(false);
            cameraNextRoom.gameObject.SetActive(true);
        }
    }
}