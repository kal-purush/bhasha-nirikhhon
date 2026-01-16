using IntegrationAdapters.Dto;
using RestSharp;
using System;
using System.Collections.Generic;

namespace Backend.Service.RequestServices
{
    public class HttpService
    {
        public static IRestResponse<List<MedicamentDto>> SendGetRequestWithRestSharp()
        {
            var client = new RestSharp.RestClient("http://localhost:8080");
            var request = new RestRequest("/medicament/Aspirin");
            var response = client.Get<List<MedicamentDto>>(request);
            Console.WriteLine("Status: " + response.StatusCode.ToString());
            List<MedicamentDto> result = response.Data;
            result.ForEach(medicament => Console.WriteLine(medicament.ToString()));
            return response;
        }
    }
}