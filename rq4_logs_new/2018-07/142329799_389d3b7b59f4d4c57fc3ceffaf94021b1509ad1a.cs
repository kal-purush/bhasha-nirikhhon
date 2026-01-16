using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace InterdimensionalThings.Services
{
    public class DataScraperService
    {
        public static void Scrape()
        {
            string serviceUrl = "https://www.datakick.org/api/items";

            //string serviceUrl = "http://www.ctabustracker.com/bustime/api/v2/getroutes?key=[APIKEY]&format=json";
            System.Net.WebRequest req = System.Net.WebRequest.Create(serviceUrl);
            System.Net.WebResponse response = req.GetResponse();
            System.IO.Stream s = response.GetResponseStream();
            System.IO.StreamReader sr = new System.IO.StreamReader(s);
            Newtonsoft.Json.JsonTextReader jsonTextReader = new Newtonsoft.Json.JsonTextReader(sr);
            Newtonsoft.Json.JsonSerializer serializer = new Newtonsoft.Json.JsonSerializer();

            DataKickObject[] result = serializer.Deserialize<DataKickObject[]>(jsonTextReader);
            //CtaResponse result = serializer.Deserialize<CtaResponse>(jsonTextReader);
            //TODO: I've pulled data in -- I should save it somewhere so it's readily available when my app needs it!

        }

        public class CtaResponse
        {
            [Newtonsoft.Json.JsonProperty(PropertyName = "bustime-response")]
            public CtaBustimes bustime { get; set; }
        }

        public class CtaBustimes
        {
            public Route[] routes { get; set; }
        }

        public class Route
        {
            public string rt { get; set; }
            public string rtnm { get; set; }
            public string rtclr { get; set; }
            public string rtdd { get; set; }
        }

        //I made this datakick object to match the response I got from the API
        public class DataKickObject
        {
            public string gtin14 { get; set; }
            public string name { get; set; }
            public string author { get; set; }
            public string format { get; set; }
            public int? pages { get; set; }
            public string[] images { get; set; }
        }
    }
}