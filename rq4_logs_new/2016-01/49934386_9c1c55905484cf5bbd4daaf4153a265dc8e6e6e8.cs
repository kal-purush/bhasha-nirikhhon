using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Device.Location;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using TaipeiOMG.Models;

namespace TaipeiOMG.Controllers
{
    public class OMGController : Controller
    {
        private static IList<Stop> stops;
        public static IList<Stop> ReadStopData(string fname)
        {
            string path = System.Web.HttpContext.Current.Server.MapPath(string.Format("~/App_Data/{0}", fname));
            string jsonText = System.IO.File.ReadAllText(path);
            JObject json = JObject.Parse(jsonText);
            IList<JToken> results = json["BusInfo"].Children().ToList();
            IList<Stop> stops = new List<Stop>();
            foreach (JToken result in results)
            {
                Stop stop = JsonConvert.DeserializeObject<Stop>(result.ToString());
                //GeoCoordinate gc = new GeoCoordinate(Double.Parse(stop.Longitude), Double.Parse(stop.Latitude));
                //stop.Coordinate = gc;
                //gc = new GeoCoordinate(Double.Parse(stop.ShowLon), Double.Parse(stop.ShowLat));
                //stop.ShowCoordinate = gc;
                stops.Add(stop);
            }

            return stops;
        }
        // GET: Stop
        public ActionResult Index()
        {
            if (stops == null)
            {
                stops = ReadStopData("GetSTOP");
            }
            return View(stops);
        }
    }
}