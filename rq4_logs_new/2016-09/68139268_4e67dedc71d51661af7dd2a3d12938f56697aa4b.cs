using System;
using System.Net;
using System.Xml.Linq;
using Rubbish.Models;

namespace Rubbish.Controllers
{
    class GeoCoder
    {
        public Address UpdateCoordinates(Address address)
        {
            var stringAddress = address.StreetNumber + " " + address.StreetName + " " + address.City + " " + address.State + " " + address.ZipCode;

            var requestUri = string.Format("http://maps.googleapis.com/maps/api/geocode/xml?address={0}&sensor=false", Uri.EscapeDataString(stringAddress));

            var request = WebRequest.Create(requestUri);
            var response = request.GetResponse();
            var xdoc = XDocument.Load(response.GetResponseStream());

            var result = xdoc.Element("GeocodeResponse").Element("result");
            var locationElement = result.Element("geometry").Element("location");
            var lat = locationElement.Element("lat");
            var lng = locationElement.Element("lng");
            string stringlat = lat.ToString();
            string stringlng = lng.ToString();
            stringlat = stringlat.Substring(5, stringlat.IndexOf("</lat>") - 5);
            stringlng = stringlng.Substring(5, stringlng.IndexOf("</lng>") - 5);


            float longitude;
            float latitude;
            float.TryParse(stringlat, out latitude);
            float.TryParse(stringlng, out longitude);

            address.Lat = latitude;
            address.Lng = longitude;

            return address;
        }
    }
}