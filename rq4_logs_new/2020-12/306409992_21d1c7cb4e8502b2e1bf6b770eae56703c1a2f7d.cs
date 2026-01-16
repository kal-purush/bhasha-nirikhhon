using HospitalMap.Service;
using HospitalMap.WPF.ModelWPF;
using System;
using System.Collections.Generic;
using System.Text;

namespace HospitalMap.Controller
{
    public class InformationEditController
    {


        private InformationEditService _informationEditService;

            public InformationEditController() {

            _informationEditService = new InformationEditService();
             }

         public void  EditInformation(RoomInformationWiev room)
        {
             _informationEditService.EditInformation(room);
        }

        public RoomInformationWiev GetRoomById(string room)
        {
            return _informationEditService.GetRoomById(room);
        }


    }
}