using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ubiDotsMockup
{
    class IFTTTConnector
    {
        public void doRequest(string eventName, string value1)
        {
            RestClient client = new RestClient("https://maker.ifttt.com/trigger/");
            var request = new RestRequest(eventName+"/with/key/bQ56hBY5W8bGxX8WAKZBz_", Method.POST);
            request.AddParameter("value1", value1); // adds to POST or URL querystring based on Method

            IRestResponse response = client.Execute(request);
            var content = response.Content;


        }
    }
}