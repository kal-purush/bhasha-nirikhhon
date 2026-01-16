using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Myles
{
    public class ChangeRoom : MonoBehaviour
    {
        public Camera cameraThisRoom;
        public Camera cameraNextRoom;
        public bool gotoNextRoomOnClick = false;

        void OnMouseDown()
        {
            if (gotoNextRoomOnClick) GotoNextRoom();
        }

        public void GotoNextRoom()
        {
            
            cameraThisRoom.gameObject.SetActive(false);
            cameraNextRoom.gameObject.SetActive(true);

        }
    }
}