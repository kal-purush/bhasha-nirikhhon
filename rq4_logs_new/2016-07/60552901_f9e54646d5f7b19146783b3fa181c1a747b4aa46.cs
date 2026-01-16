using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;

namespace ICMXamarin.Model.Api
{
    class ComputerVision
    {
        public static async void MakeRequest()
        {
            var client = new HttpClient();

            // Request headers
            client.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", "e27d0a628eee4ab8984a771eb470cc63");
            
            var uri = "https://api.projectoxford.ai/vision/v1.0/analyze?visualFeatures=Description";

            HttpResponseMessage response;

            // Request body
            byte[] byteData = Encoding.UTF8.GetBytes("{\"url\":\"https://clotildetavares.files.wordpress.com/2009/11/futebol.jpg\"}");

            using (var content = new ByteArrayContent(byteData))
            {
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                response = await client.PostAsync(uri, content);
                string resposta = await response.Content.ReadAsStringAsync();
            }

            
        }
    }
}