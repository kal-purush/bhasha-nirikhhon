using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Caughman
{
    public class ChangeRoom : MonoBehaviour
    {
        public Camera cameraThisRoom;
        public Camera cameraNextRoom;
        public bool gotoNextRoomOnClick = false;

        private void OnMouseDown()
        {
            if(gotoNextRoomOnClick) gotoNextRoom();
        }


        public void gotoNextRoom()
        {
            cameraThisRoom.gameObject.SetActive(false);
            cameraNextRoom.gameObject.SetActive(true);
        }
    }
}