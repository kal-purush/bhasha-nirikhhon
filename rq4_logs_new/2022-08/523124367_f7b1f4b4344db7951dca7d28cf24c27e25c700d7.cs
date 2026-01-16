using RestSharp;
using System;

namespace BDDAutomationFramework
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var client = new RestClient("https://reqres.in/");
            var request = new RestRequest("/api/users?page=2");
            request.AddHeader("Accept","application/json");
            request.RequestFormat = DataFormat.Json;

            RestResponse response = client.Execute(request);
            var content = response.Content;
            Console.WriteLine(content);
            Console.ReadKey();
        }
    }
}