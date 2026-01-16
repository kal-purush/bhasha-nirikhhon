using Autodesk.Revit.DB;
using System.Collections.Generic;
using System.Linq;

namespace PikTestPlugin.Models
{
    internal sealed class Apartment
    {
        public Apartment(List<SpatialElement> spatialElements)
        {
            Initialize(spatialElements);
        }

        private const string _roomPurposeParameterName = "ROM_Зона";
        private const string _roomNumberOfRoomsParameterName = "ROM_Подзона";

        public string Number { get; set; }
        public string NumberOfRooms { get; set; }
        public int RoomsCount { get; set; }
        public List<SpatialElement> Rooms { get; set; } = new List<SpatialElement>();

        private void Initialize(List<SpatialElement> spatialElements)
        {
            Number = GetApartmentNumber(spatialElements.FirstOrDefault());
            NumberOfRooms = GetNumberOfRooms(spatialElements.FirstOrDefault());
            RoomsCount = spatialElements.Count;
        }

        private string GetApartmentNumber(SpatialElement spatialElement) =>
            spatialElement.GetParameters(_roomPurposeParameterName).FirstOrDefault().AsString();
        private string GetNumberOfRooms(SpatialElement spatialElement) =>
            spatialElement.GetParameters(_roomNumberOfRoomsParameterName).FirstOrDefault().AsString();
    }
}