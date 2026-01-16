using HospitalMap.Code.Service;
using HospitalMap.WPF.ModelWPF;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace HospitalMap.Code.Controller
{
    public class SearchController
    {
        private SearchService _searchService;


        public SearchController()
        {

            _searchService = new SearchService();
        }


        public ObservableCollection<RoomInformationVieW> SearchPatientsRooms(string nameOfRoom)
        {
            return _searchService.SearchPatientsRooms(nameOfRoom);
        }


        public ObservableCollection<DoctorRoomView> SearchDoctorsRooms(string nameOfRoom)
        {
            return _searchService.SearchDoctorsRooms(nameOfRoom);
        }


        public ObservableCollection<WorkTimeViewModel> SearchAnotherRooms(string nameOfRoom)
        {
            return _searchService.SearchAnotherRooms(nameOfRoom);
        }

    }
}