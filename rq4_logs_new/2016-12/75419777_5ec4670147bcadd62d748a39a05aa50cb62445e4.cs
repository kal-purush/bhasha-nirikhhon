using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Net.Http;
using Microsoft.AspNetCore.Http;
using System.IO;

namespace eyesonthenet.Controllers
{
    public class HttpCameraAccess
    {
        // Using a static method for HttpClient reduces the build up of 'waiting' threads which can severely hinder performance
        //  http://aspnetmonsters.com/2016/08/2016-08-27-httpclientwrong/
        private static HttpClient Client = new HttpClient();

        public async Task<Stream> GetParameters()
        {
            HttpResponseMessage response = await Client.GetAsync("http://192.168.0.223/get_status.cgi");
            var stringedResponse = await response.Content.ReadAsStreamAsync();

            return stringedResponse;
        }

        public async Task<Stream> GetSnapshot()
        {
            HttpResponseMessage response = await Client.GetAsync("http://192.168.0.223/snapshot.cgi?user=mover&pwd=");
            var stringedResponse = await response.Content.ReadAsStreamAsync();

            return stringedResponse;
        }
    }
}