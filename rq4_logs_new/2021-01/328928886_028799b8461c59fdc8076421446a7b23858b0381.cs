using System.Windows.Forms;
using GMap.NET;
using GMap.NET.MapProviders;
using GMap.NET.WindowsForms;

namespace Martium.TravelInfo.App.Services
{
    public class MapService
    {
        public void InitializeMap(GMapControl map)
        {
            GMaps.Instance.Mode = AccessMode.ServerOnly;
            map.MapProvider = OpenStreetMapProvider.Instance;
            map.ShowCenter = false;
            map.DragButton = MouseButtons.Left;
        }

        public PointLatLng? GetAddressCoordinates(string fullAddress)
        {
            PointLatLng? coordinates;

            PointLatLng? foundCoordinates = GMapProviders.OpenStreetMap.GetPoint(fullAddress, out GeoCoderStatusCode status);

            if (status == GeoCoderStatusCode.OK && foundCoordinates.HasValue)
            {
                coordinates = foundCoordinates;
            }
            else
            {
                coordinates = null;
            }

            return coordinates;
        }
    }
}